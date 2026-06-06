package cluster

import "net"

// Peer is an interface tha represent the remote node
type Peer interface {
	net.Conn
	Send([]byte) error
}

// Transport is anything that can handles the communication
// between the noeds in the network. this can be of the for TCP, UDP, websocket etc.
type Transport interface {
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	ListenAddress() string
}
