package store

import (
	"io"
	"os"
)

type reader struct {
	file *os.File
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	if err == io.EOF {
		// validate checksum. If checksum is wrong then return error
	}
	return n, err
}

func (r *reader) Close() error {
	panic("implement me")
}

func (r *reader) Version() Version {
	panic("implement me")
}
