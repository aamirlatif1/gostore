package cluster

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
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
	net.Conn

	// if we dial and retrieve a connection => true otherwise false.
	outbound bool

	Wg *sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outbound: outbound,
		Wg:       &sync.WaitGroup{},
	}
}

func (p *TCPPeer) Send(data []byte) error {
	_, err := p.Conn.Write(data)
	return err
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

	log.Printf("TCP transport listen on port : %s\n", t.ListenAddr)

	return nil
}

func (t *TCPTransport) startAccptLoop() {
	for {
		conn, err := t.listerner.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("TCP accept error: %v\n", err)
		}
		go t.handleConnection(conn, false)
	}
}

// Close implements Tansport interface
func (t *TCPTransport) Close() error {
	return t.listerner.Close()
}

func (t *TCPTransport) ListenAddress() string {
	return t.ListenAddr
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	go t.handleConnection(conn, true)
	return nil
}

type RPC struct {
	From    string
	Payload []byte
}

func (t *TCPTransport) handleConnection(conn net.Conn, outbound bool) {
	var err error
	defer func() {
		fmt.Printf("dropping peer connection : %s", err)
		err = conn.Close()
	}()
	peer := NewTCPPeer(conn, outbound)

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
		rpc.From = conn.RemoteAddr().String()

		fmt.Println("waiting till stream is done")
		peer.Wg.Add(1)
		t.rpcChan <- rpc
		peer.Wg.Wait()
		fmt.Println("stream done continuing normal read loop")
	}

}
