package deebee

import (
	"errors"
	"io"
)

type OpenReader func(dir Dir) (io.ReadCloser, error)

func OpenReaderFunc(checksum Checksum) (OpenReader, error) {
	if checksum == nil {
		return nil, errors.New("")
	}
	return func(dir Dir) (io.ReadCloser, error) {
		if dir == nil {
			return nil, errors.New("nil dir")
		}
		return dir.FileReader("data")
	}, nil
}
