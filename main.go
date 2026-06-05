package main

import (
	"fmt"

	"github.com/aamirlatif1/gostore/internal/cluster"
)

func OnPeer(peer cluster.Peer) error {
	err := peer.Close()
	if err != nil {
		return err
	}
	fmt.Println("some logic outside of tcp transport")
	return nil
}

func main() {
	opts := cluster.TCPTransportOpts{
		ListenAddr:    ":4000",
		HandshakeFunc: cluster.NOOPHandshakeFunc,
		Decoder:       cluster.DefaultDecoder{},
		OnPeer:        OnPeer,
	}
	tr := cluster.NewTCPTransport(opts)
	err := tr.ListenAndAccept()
	if err != nil {
		panic("fail to start")
	}

	go func() {
		for {
			msg := <-tr.Consume()
			fmt.Printf("%+v\n", msg)
		}
	}()
	select {}
}
