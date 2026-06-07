package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

func NewEncryptionKey() []byte {
	keyBuf := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, keyBuf); err != nil {
		panic(err)
	}
	return keyBuf
}

func CopyEncrypt(key []byte, src io.Reader, dst io.Writer) (int, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return 0, err
	}

	// prepend the IV to the file.
	if _, err := dst.Write(iv); err != nil {
		return 0, err
	}

	var (
		buf    = make([]byte, 32*1024)
		stream = cipher.NewCTR(block, iv)
	)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			stream.XORKeyStream(buf, buf[:n])
			if _, err := dst.Write(buf[:n]); err != nil {
				return 0, err
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	return 0, nil
}
