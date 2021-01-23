package deebee

import (
	"io"
	"os"
	"path/filepath"
)

type OsDir string

func (o OsDir) FileReader(name string) (io.ReadCloser, error) {
	return os.Open(o.path(name))
}

func (o OsDir) FileWriter(name string) (FileWriter, error) {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	return os.OpenFile(o.path(name), flags, 0666)
}

func (o OsDir) path(name string) string {
	return filepath.Join(string(o), name)
}
