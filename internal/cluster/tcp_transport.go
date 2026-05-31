package cluster

import (
	"fmt"
	"net"
)

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
}

type TCPTransport struct {
	TCPTransportOpts
	listerner net.Listener

	// mu    sync.RWMutex
	// peers map[net.Addr]Peer
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

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
	}
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

type Message struct {
	Payload []byte
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	peer := NewTCPPeer(conn, true)

	if err := t.HandshakeFunc(peer); err != nil {
		err := conn.Close()
		if err != nil {
			fmt.Printf("TCP handshake error %s", err)
			return
		}
		fmt.Printf("TCP handshake error %s", err)
		return
	}

	// read loop
	msg := &Message{}
	for {
		if err := t.Decoder.Decode(conn, msg); err != nil {
			fmt.Printf("TCP error %s\n", err)
			continue
		}

		fmt.Printf("message : %s\n", string(msg.Payload))
	}

}
