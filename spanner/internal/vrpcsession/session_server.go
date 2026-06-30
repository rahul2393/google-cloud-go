/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vrpcsession

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc"
)

// ServeSession takes an accepted physical bidi stream and serves the provided
// grpc.Server over it. It wraps the stream into a net.Conn and feeds it to the
// grpc.Server, establishing the server side of the virtual channel.
func ServeSession(stream grpc.ServerStream, innerServer *grpc.Server) error {
	_, cancel := context.WithCancel(stream.Context())

	adapter := newStreamConnAdapter(stream, cancel)

	l := &singleConnListener{
		conn:    adapter,
		acceptC: make(chan net.Conn, 1),
	}
	l.acceptC <- adapter

	go func() {
		<-adapter.closeCh
		l.Close()
	}()

	return innerServer.Serve(l)
}

type singleConnListener struct {
	conn    net.Conn
	acceptC chan net.Conn
	once    sync.Once
}

func (s *singleConnListener) Accept() (net.Conn, error) {
	c, ok := <-s.acceptC
	if !ok {
		return nil, net.ErrClosed
	}
	return c, nil
}

func (s *singleConnListener) Close() error {
	s.once.Do(func() {
		close(s.acceptC)
	})
	return nil
}

func (s *singleConnListener) Addr() net.Addr {
	return addr{}
}
