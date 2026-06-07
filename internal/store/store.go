package store

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
)

const defaultRootPath = "../../aanetwork"

type PathTransformFunc func(string) PathKey

type PathKey struct {
	Pathname string
	Filename string
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.Pathname, p.Filename)
}

func (p PathKey) FirstPathname() string {
	paths := strings.Split(p.Pathname, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func DefaultPathTransformFunc(key string) PathKey {
	return PathKey{
		Pathname: key,
		Filename: key,
	}
}

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blockSize := 5
	sliceLength := len(hash) / blockSize
	paths := make([]string, sliceLength)
	for i := range sliceLength {
		from, to := i*blockSize, (i*blockSize)+blockSize
		paths[i] = hashStr[from:to]
	}
	return PathKey{
		Pathname: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type StoreOpts struct {
	// RootPath is the folder name of the root.
	RootPath          string
	PathTransformFunc PathTransformFunc
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.RootPath) == 0 {
		opts.RootPath = defaultRootPath
	}
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Write(key string, r io.Reader) (int64, error) {
	return s.writeStream(key, r)
}

func (s *Store) writeStream(key string, r io.Reader) (int64, error) {
	pathKey := s.PathTransformFunc(key)
	pathnameWithRoot := fmt.Sprintf("%s/%s", s.RootPath, pathKey.Pathname)
	if err := os.MkdirAll(pathnameWithRoot, os.ModePerm); err != nil {
		return 0, err
	}
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, r)
	if err != nil {
		return 0, err
	}

	fullpathWithRoot := fmt.Sprintf("%s/%s", s.RootPath, pathKey.FullPath())
	f, err := os.Create(fullpathWithRoot)
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(f, buf)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = f.Close()
	}()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf, err
}

func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullpathWithRoot := fmt.Sprintf("%s/%s", s.RootPath, pathKey.FullPath())

	f, err := os.Open(fullpathWithRoot)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)
	defer func() {
		log.Printf("deleteed [%s] from disk", pathKey.Filename)
	}()
	firstpathnameWithRoot := fmt.Sprintf("%s/%s", s.RootPath, pathKey.FirstPathname())
	return os.RemoveAll(firstpathnameWithRoot)
}

func (s *Store) Has(key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullpathWithRoot := fmt.Sprintf("%s/%s", s.RootPath, pathKey.FullPath())

	_, err := os.Stat(fullpathWithRoot)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return false
	}
	return true
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.RootPath)
}
