package cluster

import (
	"errors"
	"fmt"
	"net"
)

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts
	listerner net.Listener
	rpcChan   chan RPC
}

// TCPPeer represents the remote node over a TCP connection.
type TCPPeer struct {
	conn net.Conn

	// if we dial and retrieve a connection => true otherwise false.
	outbound bool
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn:     conn,
		outbound: outbound,
	}
}

func (p *TCPPeer) Close() error {
	return p.conn.Close()
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcChan:          make(chan RPC),
	}
}

// Consume implement the transprt interface which return read-only channel
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcChan
}

func (t *TCPTransport) ListenAndAccept() error {
	ln, err := net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}
	t.listerner = ln
	go t.startAccptLoop()
	return nil
}

func (t *TCPTransport) startAccptLoop() {
	for {
		conn, err := t.listerner.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %v\n", err)
		}
		go t.handleConnection(conn)
	}
}

type RPC struct {
	From    net.Addr
	Payload []byte
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	var err error
	defer func() {
		fmt.Printf("dropping peer connection : %s", err)
		err = conn.Close()
	}()
	peer := NewTCPPeer(conn, true)

	if err := t.HandshakeFunc(peer); err != nil {
		fmt.Printf("TCP handshake error %s", err)
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	// read loop
	rpc := RPC{}
	for {
		err = t.Decoder.Decode(conn, &rpc)
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("TCP read error %s\n", err)
			continue
		}
		rpc.From = conn.RemoteAddr()
		t.rpcChan <- rpc
	}

}
