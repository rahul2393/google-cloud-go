// Package vrpcpoc is an in-memory proof of concept showing Cloud Spanner data-plane
// calls flowing over a gRPC virtual channel (virtual RPCs multiplexed on a single
// long-lived bidirectional stream, "HTTP/2-over-HTTP/2").
//
// It runs entirely in one process against the in-memory mock Spanner server, with
// no real Cloud Frontend:
//
//   - the unreleased gRPC-Go virtual-session bits are embedded at
//     cloud.google.com/go/spanner/internal/vrpcsession (StartSessionCall +
//     ServeSession), so the POC builds against stock gRPC with no module replace;
//   - the inner server exposes both the classic google.spanner.v1.Spanner service
//     (session management) and the new spanner.cloud.frontend.bidi_vrpc.SpannerVirtualRPC
//     service (data plane), all over the one virtual channel.
//
// Tests:
//   - TestVRPCInMemorySmoke         : a single query over the virtual channel.
//   - TestVRPCMultiplexConcurrent   : 64 concurrent virtual RPCs on one physical stream.
//   - TestVRPCSpannerVirtualService : the real SpannerVirtualRPC service + Control handshake.
package vrpcpoc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/internal/testutil"
	"cloud.google.com/go/spanner/internal/vrpcsession"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// SpannerVirtualRPC service + method paths (spanner.cloud.frontend.bidi_vrpc).
const virtualService = "spanner.cloud.frontend.bidi_vrpc.SpannerVirtualRPC"

const (
	methodControl             = "/" + virtualService + "/Control"
	methodExecuteSql          = "/" + virtualService + "/ExecuteSql"
	methodExecuteStreamingSql = "/" + virtualService + "/ExecuteStreamingSql"
	methodStreamingRead       = "/" + virtualService + "/StreamingRead"
	methodBeginTransaction    = "/" + virtualService + "/BeginTransaction"
	methodCommit              = "/" + virtualService + "/Commit"
	methodRollback            = "/" + virtualService + "/Rollback"
)

// rawPassthrough is the OUTER physical-stream codec. The outer stream only ever
// carries []byte (the init message plus the inner HTTP/2 frames). Installed via
// grpc.ForceServerCodec; never registered globally (that would clobber the proto
// codec the inner Spanner server uses).
type rawPassthrough struct{}

func (rawPassthrough) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("outer codec: want []byte, got %T", v)
	}
	return b, nil
}
func (rawPassthrough) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("outer codec: want *[]byte, got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}
func (rawPassthrough) Name() string { return "proto" }

// virtualStreamingServer adapts a raw grpc.ServerStream to the generated
// Spanner_ExecuteStreamingSqlServer / Spanner_StreamingReadServer interfaces so
// the in-memory mock can write PartialResultSets straight to the virtual RPC.
type virtualStreamingServer struct {
	grpc.ServerStream
}

func (s virtualStreamingServer) Send(m *spannerpb.PartialResultSet) error {
	return s.ServerStream.SendMsg(m)
}

// spannerVirtualRPCHandler serves the SpannerVirtualRPC data-plane methods by
// delegating to the in-memory mock (which implements the classic Spanner server).
func spannerVirtualRPCHandler(mock spannerpb.SpannerServer) grpc.StreamHandler {
	return func(_ any, stream grpc.ServerStream) error {
		method, ok := grpc.MethodFromServerStream(stream)
		if !ok {
			return status.Error(codes.Internal, "no method on virtual server stream")
		}
		switch method {
		case methodControl:
			// First RPC on the service: empty InitRequest -> empty InitResponse.
			var init emptypb.Empty
			if err := stream.RecvMsg(&init); err != nil {
				return err
			}
			return stream.SendMsg(&emptypb.Empty{})

		case methodExecuteStreamingSql:
			var req spannerpb.ExecuteSqlRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			return mock.ExecuteStreamingSql(&req, virtualStreamingServer{stream})

		case methodStreamingRead:
			var req spannerpb.ReadRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			return mock.StreamingRead(&req, virtualStreamingServer{stream})

		case methodExecuteSql:
			var req spannerpb.ExecuteSqlRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			resp, err := mock.ExecuteSql(stream.Context(), &req)
			if err != nil {
				return err
			}
			return stream.SendMsg(resp)

		case methodBeginTransaction:
			var req spannerpb.BeginTransactionRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			resp, err := mock.BeginTransaction(stream.Context(), &req)
			if err != nil {
				return err
			}
			return stream.SendMsg(resp)

		case methodCommit:
			var req spannerpb.CommitRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			resp, err := mock.Commit(stream.Context(), &req)
			if err != nil {
				return err
			}
			return stream.SendMsg(resp)

		case methodRollback:
			var req spannerpb.RollbackRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			if _, err := mock.Rollback(stream.Context(), &req); err != nil {
				return err
			}
			return stream.SendMsg(&emptypb.Empty{})

		default:
			return status.Errorf(codes.Unimplemented, "SpannerVirtualRPC method %q not implemented", method)
		}
	}
}

func seedSelect1(mock testutil.InMemSpannerServer) {
	rs := &spannerpb.ResultSet{
		Metadata: &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{
				Fields: []*spannerpb.StructType_Field{
					{Name: "c", Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64}},
				},
			},
		},
		Rows: []*structpb.ListValue{
			{Values: []*structpb.Value{{Kind: &structpb.Value_StringValue{StringValue: "1"}}}},
		},
	}
	mock.PutStatementResult("SELECT 1", &testutil.StatementResult{
		Type:      testutil.StatementResultResultSet,
		ResultSet: rs,
	})
}

// newVRPCConn spins an in-process mock Spanner hosted over a virtual channel and
// returns the inner VirtualConn. The inner server serves both the classic Spanner
// service (session management) and the SpannerVirtualRPC service (data plane).
func newVRPCConn(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()

	mock := testutil.NewInMemSpannerServer()
	seedSelect1(mock)

	inner := grpc.NewServer(grpc.UnknownServiceHandler(spannerVirtualRPCHandler(mock)))
	spannerpb.RegisterSpannerServer(inner, mock)

	outerLis := bufconn.Listen(1 << 20)
	outer := grpc.NewServer(
		grpc.ForceServerCodec(rawPassthrough{}),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			var initReq []byte
			if err := stream.RecvMsg(&initReq); err != nil {
				return err
			}
			if err := stream.SendHeader(nil); err != nil { // -> client Ack
				return err
			}
			return vrpcsession.ServeSession(stream, inner)
		}),
	)
	go outer.Serve(outerLis)

	outerConn, err := grpc.NewClient("passthrough:///outer",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return outerLis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial outer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	sess, err := vrpcsession.StartSessionCall(ctx, outerConn, "/google.spanner.v1.Spanner/Session", []byte("vrpc-init"), nil)
	if err != nil {
		cancel()
		t.Fatalf("StartSessionCall: %v", err)
	}
	select {
	case err := <-sess.Ack:
		if err != nil {
			cancel()
			t.Fatalf("session Ack error: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("timeout waiting for session Ack")
	}

	cleanup := func() {
		cancel()
		sess.VirtualConn.Close()
		outerConn.Close()
		outer.Stop()
		inner.Stop()
	}
	return sess.VirtualConn, cleanup
}

// createSession opens a Spanner session over the classic service on the virtual channel.
func createSession(ctx context.Context, t *testing.T, vconn *grpc.ClientConn) string {
	t.Helper()
	cs, err := spannerpb.NewSpannerClient(vconn).CreateSession(ctx, &spannerpb.CreateSessionRequest{
		Database: "projects/p/instances/i/databases/d",
	})
	if err != nil {
		t.Fatalf("CreateSession over vRPC: %v", err)
	}
	return cs.Name
}

// TestVRPCInMemorySmoke: a single query over the virtual channel end to end.
func TestVRPCInMemorySmoke(t *testing.T) {
	vconn, cleanup := newVRPCConn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := createSession(ctx, t, vconn)
	t.Logf("created session over virtual channel: %s", session)

	stream, err := spannerpb.NewSpannerClient(vconn).ExecuteStreamingSql(ctx, &spannerpb.ExecuteSqlRequest{
		Session: session,
		Sql:     "SELECT 1",
	})
	if err != nil {
		t.Fatalf("ExecuteStreamingSql over vRPC: %v", err)
	}
	var values int
	for {
		prs, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv over vRPC: %v", err)
		}
		values += len(prs.GetValues())
	}
	if values == 0 {
		t.Fatal("got 0 values from streaming SELECT 1 over vRPC; want >=1")
	}
	t.Logf("PASS: received %d value(s) for SELECT 1 over the virtual channel", values)
}

// TestVRPCMultiplexConcurrent: many concurrent virtual RPCs on ONE physical stream.
func TestVRPCMultiplexConcurrent(t *testing.T) {
	vconn, cleanup := newVRPCConn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	session := createSession(ctx, t, vconn)
	sc := spannerpb.NewSpannerClient(vconn)

	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				if _, err := sc.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, Sql: "SELECT 1"}); err != nil {
					errs <- fmt.Errorf("unary[%d]: %w", i, err)
				}
				return
			}
			stream, err := sc.ExecuteStreamingSql(ctx, &spannerpb.ExecuteSqlRequest{Session: session, Sql: "SELECT 1"})
			if err != nil {
				errs <- fmt.Errorf("stream[%d] open: %w", i, err)
				return
			}
			for {
				if _, err := stream.Recv(); err == io.EOF {
					break
				} else if err != nil {
					errs <- fmt.Errorf("stream[%d] recv: %w", i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	var failed int
	for err := range errs {
		failed++
		t.Errorf("concurrent vRPC failed: %v", err)
	}
	if failed == 0 {
		t.Logf("PASS: %d concurrent virtual RPCs multiplexed over one physical stream, all succeeded", n)
	}
}

// TestVRPCSpannerVirtualService exercises the real SpannerVirtualRPC service:
// the mandatory Control handshake first, then a data-plane query over the
// SpannerVirtualRPC/ExecuteStreamingSql method path.
func TestVRPCSpannerVirtualService(t *testing.T) {
	vconn, cleanup := newVRPCConn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Session management stays on the classic service.
	session := createSession(ctx, t, vconn)

	// Control must be the first RPC on the SpannerVirtualRPC service.
	control, err := vconn.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Control",
		ClientStreams: true,
		ServerStreams: true,
	}, methodControl)
	if err != nil {
		t.Fatalf("open Control stream: %v", err)
	}
	if err := control.SendMsg(&emptypb.Empty{}); err != nil { // ClientControl.InitRequest
		t.Fatalf("Control SendMsg: %v", err)
	}
	var initResp emptypb.Empty
	if err := control.RecvMsg(&initResp); err != nil { // ServerControl.InitResponse
		t.Fatalf("Control handshake: %v", err)
	}
	t.Log("SpannerVirtualRPC Control handshake complete")

	// Data plane over SpannerVirtualRPC/ExecuteStreamingSql.
	stream, err := vconn.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "ExecuteStreamingSql",
		ServerStreams: true,
	}, methodExecuteStreamingSql)
	if err != nil {
		t.Fatalf("open ExecuteStreamingSql vRPC: %v", err)
	}
	if err := stream.SendMsg(&spannerpb.ExecuteSqlRequest{Session: session, Sql: "SELECT 1"}); err != nil {
		t.Fatalf("vRPC SendMsg: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("vRPC CloseSend: %v", err)
	}
	var values int
	for {
		var prs spannerpb.PartialResultSet
		if err := stream.RecvMsg(&prs); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("vRPC RecvMsg: %v", err)
		}
		values += len(prs.GetValues())
	}
	if values == 0 {
		t.Fatal("got 0 values from SpannerVirtualRPC/ExecuteStreamingSql; want >=1")
	}
	t.Logf("PASS: %d value(s) for SELECT 1 over %s/ExecuteStreamingSql", values, virtualService)
}
