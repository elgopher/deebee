package deebee

import (
	"io"
)

type NewChecksum func() Checksum

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

type openWriter func(key string) (io.WriteCloser, error)

func openWriterFunc(dir Dir, newChecksum NewChecksum) (openWriter, error) {
	return func(key string) (io.WriteCloser, error) {
		return dir.FileWriter(key)
	}, nil
}
