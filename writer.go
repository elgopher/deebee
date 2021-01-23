package deebee

import (
	"errors"
	"io"
)

type Checksum interface {
	Add(b []byte)
	Calculate() uint32
}

type Dir interface {
	// Opens an existing file for read. Must return error when file does not exist
	FileReader(name string) (io.ReadCloser, error)
	// Creates a new file for write. Must return error when file already exists
	FileWriter(name string) (FileWriter, error)
}

type FileWriter interface {
	io.WriteCloser
	Sync() error
}

type OpenWriter func(dir Dir) (io.WriteCloser, error)

func OpenWriterFunc(checksum Checksum) (OpenWriter, error) {
	if checksum == nil {
		return nil, errors.New("")
	}

	return func(dir Dir) (io.WriteCloser, error) {
		if dir == nil {
			return nil, errors.New("nil dir")
		}
		return dir.FileWriter("data")
	}, nil
}
