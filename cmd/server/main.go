package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aamirlatif1/gostore/internal/cluster"
	"github.com/aamirlatif1/gostore/internal/crypto"
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
		EncKey:            crypto.NewEncryptionKey(),
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

	go func() {
		if err := s.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := s2.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("coolpicture_%d.jpg", i)

		data := bytes.NewReader([]byte("my big data file here!"))
		s2.Store(key, data)
		time.Sleep(5 * time.Millisecond)

		// if err := s2.store.Delete(key); err != nil {
		// 	log.Fatal(err)
		// }

		r, err := s.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(time.Millisecond)
		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("file data here : %s\n", string(b))
	}

}
