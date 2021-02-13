package store

import "io"

// Dir is a filesystem abstraction useful for unit testing and decoupling the code from `os` package.
//
// Names with file separators are not supported
type Dir interface {
	// FileReader opens an existing file for read. Must return error when file does not exist
	FileReader(name string) (io.ReadCloser, error)
	// FileWriter creates a new file for write. Must return error when file already exists
	FileWriter(name string) (FileWriter, error)
	// Mkdir creates this directory. Do nothing when directory already exists
	Mkdir() error
	// Dir returns directory with name. Does not check immediately if dir exists.
	Dir(name string) Dir
	// Exists returns true when directory exists
	Exists() (bool, error)
	// ListFiles list files excluding directories
	ListFiles() ([]string, error)
	// DeleteFile does not return error when file does not exist
	DeleteFile(name string) error
}

type FileWriter interface {
	io.WriteCloser
	Sync() error
}
