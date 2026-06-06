package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/aamirlatif1/gostore/internal/cluster"
	"github.com/aamirlatif1/gostore/internal/store"
)

type FileServerOpts struct {
	ListenAddr        string
	StorageRoot       string
	PathTransformFunc store.PathTransformFunc
	Transport         cluster.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]cluster.Peer

	store  store.Store
	quitCh chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := store.StoreOpts{
		RootPath:          opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}
	return &FileServer{
		FileServerOpts: opts,
		store:          *store.NewStore(storeOpts),
		quitCh:         make(chan struct{}),
		peers:          make(map[string]cluster.Peer),
	}
}

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	if len(s.BootstrapNodes) != 0 {
		err := s.bootstrapNetwork()
		if err != nil {
			return err
		}
	}
	s.loop()

	return nil
}

func (s *FileServer) loop() {
	defer func() {
		log.Println("file server stopped.")
		err := s.Transport.Close()
		if err != nil {
			log.Println("fail to close transport : ", err)
		}
	}()
	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println(err)
			}

			fmt.Printf("recd: %s\n", string(msg.Payload.([]byte)))

			peer, ok := s.peers[rpc.From]
			if !ok {
				panic("peer not found in peer map")
			}

			fmt.Printf("%+v", peer)

			b := make([]byte, 1000)
			if _, err := peer.Read(b); err != nil {
				panic(err)
			}

			fmt.Printf("recv: %s\n", string(b))
			// if err := s.handleMessage(&m); err != nil {
			// 	log.Println(err)
			// }
		case <-s.quitCh:
			return
		}
	}
}

// func (s *FileServer) handleMessage(msg *Message) error {

// 	return nil
// }

func (s *FileServer) Stop() {
	close(s.quitCh)
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial err: ", err)
			}
		}(addr)
	}
	return nil
}

func (s *FileServer) OnPeer(p cluster.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connected with remote peer: %s\n", p.RemoteAddr())

	return nil
}

func (s *FileServer) broadcast(p *Message) error {
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	mw := io.MultiWriter(peers...)
	return gob.NewEncoder(mw).Encode(p)
}

type Message struct {
	From    string
	Payload any
}

func (s *FileServer) StoreData(key string, r io.Reader) error {
	// 1. Store this file on disk
	// 2. broadcase this file to all known peers in the network

	buf := new(bytes.Buffer)
	msg := &Message{
		Payload: []byte("storagekey"),
	}
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	payload := []byte("THIS IS LARGE FILE")
	for _, peer := range s.peers {
		if err := peer.Send(payload); err != nil {
			return err
		}
	}

	// buf := new(bytes.Buffer)
	// tee := io.TeeReader(r, buf)

	// if err := s.store.Write(key, tee); err != nil {
	// 	return err
	// }

	// _, err := io.Copy(buf, r)
	// if err != nil {
	// 	return err
	// }

	return nil
}
