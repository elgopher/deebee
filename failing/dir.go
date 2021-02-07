// Package failing provides methods decorating another Dir with errors
package failing

import (
	"errors"
	"io"

	"github.com/jacekolszak/deebee/store"
)

func Exists(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.exists = func() (bool, error) {
		return false, errors.New("exists failed")
	}
	dir.dir = func(name string) store.Dir {
		return Mkdir(decoratedDir.Dir(name))
	}
	return dir
}

func Mkdir(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.mkdir = func() error {
		return errors.New("mkdir failed")
	}
	dir.dir = func(name string) store.Dir {
		return Mkdir(decoratedDir.Dir(name))
	}
	return dir
}

func FileWriter(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.fileWriter = func(name string) (store.FileWriter, error) {
		return nil, errors.New("fileWriter failed")
	}
	dir.dir = func(name string) store.Dir {
		return FileWriter(decoratedDir.Dir(name))
	}
	return dir
}

func FileReader(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.fileReader = func(name string) (io.ReadCloser, error) {
		return nil, errors.New("fileReader failed")
	}
	dir.dir = func(name string) store.Dir {
		return FileReader(decoratedDir.Dir(name))
	}
	return dir
}

func ListFiles(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.listFiles = func() ([]string, error) {
		return nil, errors.New("listFiles failed")
	}
	dir.dir = func(name string) store.Dir {
		return ListFiles(decoratedDir.Dir(name))
	}
	return dir
}

func DeleteFile(decoratedDir store.Dir) store.Dir {
	dir := decorate(decoratedDir)
	dir.deleteFile = func(name string) error {
		return errors.New("deleteFile failed")
	}
	dir.dir = func(name string) store.Dir {
		return ListFiles(decoratedDir.Dir(name))
	}
	return dir
}

func decorate(dir store.Dir) *failingDir {
	return &failingDir{
		fileReader: dir.FileReader,
		fileWriter: dir.FileWriter,
		mkdir:      dir.Mkdir,
		exists:     dir.Exists,
		listFiles:  dir.ListFiles,
		deleteFile: dir.DeleteFile,
	}
}

type failingDir struct {
	fileReader func(name string) (io.ReadCloser, error)
	fileWriter func(name string) (store.FileWriter, error)
	mkdir      func() error
	dir        func(name string) store.Dir
	exists     func() (bool, error)
	listFiles  func() ([]string, error)
	deleteFile func(name string) error
}

func (d *failingDir) FileReader(name string) (io.ReadCloser, error) {
	return d.fileReader(name)
}

func (d *failingDir) FileWriter(name string) (store.FileWriter, error) {
	return d.fileWriter(name)
}

func (d *failingDir) Mkdir() error {
	return d.mkdir()
}

func (d *failingDir) Dir(name string) store.Dir {
	return d.dir(name)
}

func (d *failingDir) Exists() (bool, error) {
	return d.exists()
}

func (d *failingDir) ListFiles() ([]string, error) {
	return d.listFiles()
}

func (d *failingDir) DeleteFile(name string) error {
	return d.deleteFile(name)
}
