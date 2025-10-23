package grpc

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type pooledClient struct {
	mu      sync.Mutex
	conn    *grpc.ClientConn
	refs    int
	closing bool
	key     dialerConf
}

func newPooledClient(conn *grpc.ClientConn, key dialerConf) *pooledClient {
	return &pooledClient{
		conn: conn,
		key:  key,
	}
}

func (p *pooledClient) acquire() (*managedClientConn, bool) {
	p.mu.Lock()

	if p.conn == nil || p.closing {
		p.mu.Unlock()
		return nil, false
	}

	state := p.conn.GetState()
	if state == connectivity.Shutdown || state == connectivity.TransientFailure {
		conn := p.conn
		p.conn = nil
		p.closing = true
		p.refs = 0
		p.mu.Unlock()
		conn.Close()
		clientConnCache.Delete(p.key)
		return nil, false
	}

	p.refs++
	conn := p.conn
	p.mu.Unlock()

	return &managedClientConn{
		ClientConnInterface: conn,
		parent:              p,
	}, true
}

func (p *pooledClient) release() {
	var toClose *grpc.ClientConn

	p.mu.Lock()
	if p.refs > 0 {
		p.refs--
	}
	if p.refs == 0 && p.conn != nil {
		toClose = p.conn
		p.conn = nil
		p.closing = true
	}
	p.mu.Unlock()

	if toClose != nil {
		toClose.Close()
		clientConnCache.Delete(p.key)
	}
}

type managedClientConn struct {
	grpc.ClientConnInterface
	parent *pooledClient
	once   sync.Once
}

func (m *managedClientConn) release() {
	m.once.Do(func() {
		m.parent.release()
	})
}

func (m *managedClientConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	err := m.ClientConnInterface.Invoke(ctx, method, args, reply, opts...)
	m.release()
	return err
}

func (m *managedClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := m.ClientConnInterface.NewStream(ctx, desc, method, opts...)
	if err != nil {
		m.release()
		return nil, err
	}
	return &managedClientStream{
		ClientStream: stream,
		release:      m.release,
	}, nil
}

type managedClientStream struct {
	grpc.ClientStream
	release func()
	once    sync.Once
}

func (s *managedClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	s.once.Do(s.release)
	return err
}

func (s *managedClientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.once.Do(s.release)
	}
	return err
}

func (s *managedClientStream) SendMsg(m interface{}) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.once.Do(s.release)
	}
	return err
}
