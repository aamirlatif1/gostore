package main

import (
	"github.com/aamirlatif1/gostore/internal/cluster"
)

func main() {
	opts := cluster.TCPTransportOpts{
		ListenAddr:    ":4000",
		HandshakeFunc: cluster.NOOPHandshakeFunc,
		Decoder:       cluster.DefaultDecoder{},
	}
	tr := cluster.NewTCPTransport(opts)
	err := tr.ListenAndAccept()
	if err != nil {
		panic("fail to start")
	}
	select {}
}
