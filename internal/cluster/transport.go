package cluster

// Peer is an interface tha represent the remote node
type Peer interface {
}

// Transport is anything that can handles the communication
// between the noeds in the network. this can be of the for TCP, UDP, websocket etc.
type Transport interface {
	ListenAndAccept() error
}
