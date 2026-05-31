package cluster

import "errors"

var ErrInvalidHandshake = errors.New("invalid handshake")

type HandshakeFunc func(Peer) error

func NOOPHandshakeFunc(Peer) error { return nil }
