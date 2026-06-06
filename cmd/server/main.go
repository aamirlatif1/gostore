package main

import (
	"bytes"
	"log"
	"time"

	"github.com/aamirlatif1/gostore/internal/cluster"
	"github.com/aamirlatif1/gostore/internal/store"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := cluster.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: cluster.NOOPHandshakeFunc,
		Decoder:       cluster.DefaultDecoder{},
		// TODO: onPeer func
	}

	tcpTransport := cluster.NewTCPTransport(tcpTransportOpts)
	fileserverOpts := FileServerOpts{
		ListenAddr:        listenAddr,
		StorageRoot:       listenAddr + "_network",
		PathTransformFunc: store.CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileserverOpts)
	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {

	s := makeServer(":4000")
	s2 := makeServer(":3000", ":4000")

	// go func() {
	// 	time.Sleep(2 * time.Second)
	// 	s.Stop()
	// }()

	go func() {
		if err := s.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	go s2.Start()

	time.Sleep(2 * time.Second)

	data := bytes.NewReader([]byte("my big data file here!"))

	s2.StoreData("myprivatefile", data)

	select {}

}
