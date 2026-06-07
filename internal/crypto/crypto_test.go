package crypto_test

import (
	"bytes"
	"testing"

	"github.com/aamirlatif1/gostore/internal/crypto"
)

func TestCopyEncrptAndDecrypt(t *testing.T) {
	payload := "Foo not Bar"
	src := bytes.NewReader([]byte(payload))
	dst := new(bytes.Buffer)
	key := crypto.NewEncryptionKey()

	_, err := crypto.CopyEncrypt(key, src, dst)

	if err != nil {
		t.Error("error not expected", err)
	}

	out := new(bytes.Buffer)
	nw, err := crypto.CopyDecrypt(key, dst, out)
	if err != nil {
		t.Error(err)
	}
	if nw != 16+len(payload) {
		t.Errorf("decrypted message length not matching, expeted %d actual %d", 16+len(payload), nw)
	}

	if out.String() != payload {
		t.Errorf("\nmessage encryption and decryption do not match, expect %s actual %s", payload, out.String())
	}
}
