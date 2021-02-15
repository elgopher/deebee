// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package os

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jacekolszak/deebee/store"
)

type Dir string

func (o Dir) FileReader(name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	return os.Open(o.path(name))
}

func (o Dir) FileWriter(name string) (store.FileWriter, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	return os.OpenFile(o.path(name), flags, 0664)
}

func (o Dir) path(name string) string {
	return filepath.Join(string(o), name)
}

func (o Dir) Exists() (bool, error) {
	f, err := os.Stat(string(o))
	if os.IsNotExist(err) {
		return false, nil
	}
	return f.IsDir(), nil
}

func (o Dir) Mkdir() error {
	err := os.Mkdir(string(o), 0775)
	if os.IsExist(err) {
		return nil
	}
	return err
}

func (o Dir) Dir(name string) store.Dir {
	return Dir(o.path(name))
}

func (o Dir) ListFiles() ([]string, error) {
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

func (o Dir) DeleteFile(name string) error {
	err := os.Remove(o.path(name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
