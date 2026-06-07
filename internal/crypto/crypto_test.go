package crypto_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aamirlatif1/gostore/internal/crypto"
)

func TestCopyEncrpt(t *testing.T) {
	src := bytes.NewReader([]byte("Foo not Bar"))
	dst := new(bytes.Buffer)
	key := crypto.NewEncryptionKey()

	_, err := crypto.CopyEncrypt(key, src, dst)

	if err != nil {
		t.Error("error not expected", err)
	}

	fmt.Println(dst.String())
}
