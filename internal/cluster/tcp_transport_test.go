package cluster_test

import (
	"testing"

	"github.com/aamirlatif1/gostore/internal/cluster"
)

func Test(t *testing.T) {
	opts := cluster.TCPTransportOpts{
		ListenAddr:    ":4000",
		HandshakeFunc: cluster.NOOPHandshakeFunc,
		Decoder:       cluster.DefaultDecoder{},
	}
	tr := cluster.NewTCPTransport(opts)

	if tr.ListenAddr != opts.ListenAddr {
		t.Errorf("\nlisten address does not matche, actual %q expected %q", tr.ListenAddr, opts.ListenAddr)
	}
	err := tr.ListenAndAccept()

	if err != nil {
		t.Error("fail to listen and accept", err)
	}
}
