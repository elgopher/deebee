// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

// Package failing provides methods decorating another Dir with errors
package failing

import (
	"errors"
	"io"
	"sync"

	"github.com/jacekolszak/deebee/store"
)

func Exists(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.exists = func() (bool, error) {
		return false, errors.New("exists failed")
	}
	dir.dir = func(name string) store.Dir {
		return Exists(decoratedDir.Dir(name))
	}
	return dir
}

func Mkdir(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.mkdir = func() error {
		return errors.New("mkdir failed")
	}
	dir.dir = func(name string) store.Dir {
		return Mkdir(decoratedDir.Dir(name))
	}
	return dir
}

func FileWriter(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.fileWriter = func(name string) (store.FileWriter, error) {
		return nil, errors.New("fileWriter failed")
	}
	dir.dir = func(name string) store.Dir {
		return FileWriter(decoratedDir.Dir(name))
	}
	return dir
}

func FileReader(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.fileReader = func(name string) (io.ReadCloser, error) {
		return nil, errors.New("fileReader failed")
	}
	dir.dir = func(name string) store.Dir {
		return FileReader(decoratedDir.Dir(name))
	}
	return dir
}

func ListFiles(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.listFiles = func() ([]string, error) {
		return nil, errors.New("listFiles failed")
	}
	dir.dir = func(name string) store.Dir {
		return ListFiles(decoratedDir.Dir(name))
	}
	return dir
}

func DeleteFile(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	dir.deleteFile = func(name string) error {
		return errors.New("deleteFile failed")
	}
	dir.dir = func(name string) store.Dir {
		return DeleteFile(decoratedDir.Dir(name))
	}
	return dir
}

func DeleteFileOnce(decoratedDir store.Dir) *Dir {
	dir := DecorateDir(decoratedDir)
	once := sync.Once{}
	dir.deleteFile = func(name string) error {
		var shouldFail bool
		once.Do(func() {
			shouldFail = true
		})
		if shouldFail {
			return errors.New("deleteFile failed")
		}
		return decoratedDir.DeleteFile(name)
	}
	dir.dir = func(name string) store.Dir {
		return DeleteFile(decoratedDir.Dir(name))
	}
	return dir
}

func DecorateDir(dir store.Dir) *Dir {
	return &Dir{
		fileReader: dir.FileReader,
		fileWriter: dir.FileWriter,
		mkdir:      dir.Mkdir,
		exists:     dir.Exists,
		listFiles:  dir.ListFiles,
		deleteFile: dir.DeleteFile,
	}
}

type Dir struct {
	fileReader func(name string) (io.ReadCloser, error)
	fileWriter func(name string) (store.FileWriter, error)
	mkdir      func() error
	dir        func(name string) store.Dir
	exists     func() (bool, error)
	listFiles  func() ([]string, error)
	deleteFile func(name string) error
}

func (d *Dir) FileReader(name string) (io.ReadCloser, error) {
	return d.fileReader(name)
}

func (d *Dir) FileWriter(name string) (store.FileWriter, error) {
	return d.fileWriter(name)
}

func (d *Dir) Mkdir() error {
	return d.mkdir()
}

func (d *Dir) Dir(name string) store.Dir {
	return d.dir(name)
}

func (d *Dir) Exists() (bool, error) {
	return d.exists()
}

func (d *Dir) ListFiles() ([]string, error) {
	return d.listFiles()
}

func (d *Dir) DeleteFile(name string) error {
	return d.deleteFile(name)
}
