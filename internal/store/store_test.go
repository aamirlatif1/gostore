package store_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/aamirlatif1/gostore/internal/store"
)

func TestPahtTransformFunc(t *testing.T) {
	key := "monsbestpicture"
	pathname := store.CASPathTransformFunc(key)
	expectedPath := "a4b2a/86891/16ae4/575f2"
	expectedFileName := "a4b2a8689116ae4575f2"
	if pathname.Pathname != expectedPath {
		t.Errorf("path name did not match, actual %q expected %q", pathname.Pathname, expectedPath)
	}
	if pathname.Pathname != expectedPath {
		t.Errorf("file name did not match, actual %q expected %q", pathname.Filename, expectedFileName)
	}
}

func TestStore(t *testing.T) {
	s := setupStore(t)

	for i := range 50 {
		key := fmt.Sprintf("monsbestpicture_%d", i)
		data := []byte("some jpg bytes")
		if _, err := s.Write(key, bytes.NewReader(data)); err != nil {
			t.Error(err)
		}

		r, err := s.Read(key)
		if err != nil {
			t.Error(err)
		}

		b, _ := io.ReadAll(r)

		if string(b) != string(data) {
			t.Errorf("read content did not match, want %q got %q", data, b)
		}
	}

}

func TestDeleteFile(t *testing.T) {
	key := "myspacialpic"
	opts := store.StoreOpts{
		PathTransformFunc: store.CASPathTransformFunc,
	}
	s := store.NewStore(opts)

	data := []byte("some jpg bytes")
	if _, err := s.Write(key, bytes.NewReader(data)); err != nil {
		t.Error(err)
	}

	err := s.Delete(key)
	if err != nil {
		t.Error(err)
	}
	fileExist := s.Has(key)
	if fileExist {
		t.Error("file should be deleted")
	}
}

func setupStore(t testing.TB) *store.Store {
	t.Helper()
	opts := store.StoreOpts{
		PathTransformFunc: store.CASPathTransformFunc,
	}
	s := store.NewStore(opts)
	t.Cleanup(func() {
		err := s.Clear()
		if err != nil {
			t.Error(err)
		}
	})
	return s
}
