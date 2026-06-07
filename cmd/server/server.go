package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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
		log.Println("file server stopped due to error or user quit action.")
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
				log.Println("decoding error : ", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				fmt.Println("handle message error : ", err)
			}
		case <-s.quitCh:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	}
	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) but it does not exist on dick", s.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serveing file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.store.Read(msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readcloser")
		defer func() {
			if err := rc.Close(); err != nil {
				log.Println("close err: ", err)
			}
		}()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	// first send IncomingStream then file size
	if err := peer.Send([]byte{cluster.IncomingStream}); err != nil {
		return err
	}
	if err := binary.Write(peer, binary.LittleEndian, fileSize); err != nil {
		return err
	}

	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) bytes over the network to %s\n", s.Transport.Addr(), n, from)
	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) cound not be found in peer list", from)
	}

	n, err := s.store.Write(msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes to disk\n", s.Transport.Addr(), n)
	peer.CloseStream()

	return nil
}

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

func (s *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		if err := peer.Send([]byte{cluster.IncomingMessage}); err != nil {
			return err
		}
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)
		_, f, err := s.store.Read(key)
		return f, err
	}

	fmt.Printf("[%s] dont have file (%s) locally fetching from network\n", s.Transport.Addr(), key)

	msg := Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Microsecond * 50)

	for _, peer := range s.peers {
		// first read file size so we can limit stream
		var fileSize int64
		if err := binary.Read(peer, binary.LittleEndian, &fileSize); err != nil {
			return nil, err
		}
		n, err := s.store.Write(key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		fmt.Printf("[%s] recieved ([)%d) bytes over the network from %s", s.Transport.Addr(), n, peer.RemoteAddr())
		peer.CloseStream()
	}

	_, f, err := s.store.Read(key)
	return f, err
}

func (s *FileServer) Store(key string, r io.Reader) error {
	var (
		fileBuf = new(bytes.Buffer)
		tee     = io.TeeReader(r, fileBuf)
	)

	size, err := s.store.Write(key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			Key:  key,
			Size: size,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(5 * time.Millisecond)

	// TODO: use a multiwriter here.
	for _, peer := range s.peers {
		if err := peer.Send([]byte{cluster.IncomingStream}); err != nil {
			return err
		}
		n, err := io.Copy(peer, fileBuf)
		if err != nil {
			return err
		}
		fmt.Printf("received and written bytes to disk %d\n", n)
	}

	return nil
}
