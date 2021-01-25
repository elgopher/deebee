package deebee

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type OsDir string

func (o OsDir) FileReader(name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	return os.Open(o.path(name))
}

func (o OsDir) FileWriter(name string) (FileWriter, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	return os.OpenFile(o.path(name), flags, 0664)
}

func (o OsDir) path(name string) string {
	return filepath.Join(string(o), name)
}

func (o OsDir) Exists() (bool, error) {
	f, err := os.Stat(string(o))
	if os.IsNotExist(err) {
		return false, nil
	}
	return f.IsDir(), nil
}

func (o OsDir) Mkdir(name string) error {
	err := os.Mkdir(o.path(name), 0775)
	if os.IsExist(err) {
		return nil
	}
	return err
}

func (o OsDir) Dir(name string) Dir {
	return OsDir(o.path(name))
}

func (o OsDir) ListFiles() ([]string, error) {
	var files []string
	fileInfos, err := ioutil.ReadDir(string(o))
	if err != nil {
		return nil, err
	}
	for _, f := range fileInfos {
		if !f.IsDir() {
			files = append(files, f.Name())
		}
	}
	return files, nil
}
