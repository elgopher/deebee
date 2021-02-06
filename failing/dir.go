// Package failing provides methods decorating another Dir with errors
package failing

import (
	"errors"
	"io"

	"github.com/jacekolszak/deebee"
)

func Exists(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.exists = func() (bool, error) {
		return false, errors.New("exists failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return Mkdir(decoratedDir.Dir(name))
	}
	return dir
}

func Mkdir(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.mkdir = func() error {
		return errors.New("mkdir failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return Mkdir(decoratedDir.Dir(name))
	}
	return dir
}

func FileWriter(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.fileWriter = func(name string) (deebee.FileWriter, error) {
		return nil, errors.New("fileWriter failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return FileWriter(decoratedDir.Dir(name))
	}
	return dir
}

func FileReader(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.fileReader = func(name string) (io.ReadCloser, error) {
		return nil, errors.New("fileReader failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return FileReader(decoratedDir.Dir(name))
	}
	return dir
}

func ListFiles(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.listFiles = func() ([]string, error) {
		return nil, errors.New("listFiles failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return ListFiles(decoratedDir.Dir(name))
	}
	return dir
}

func DeleteFile(decoratedDir deebee.Dir) deebee.Dir {
	dir := decorate(decoratedDir)
	dir.deleteFile = func(name string) error {
		return errors.New("deleteFile failed")
	}
	dir.dir = func(name string) deebee.Dir {
		return ListFiles(decoratedDir.Dir(name))
	}
	return dir
}

func decorate(dir deebee.Dir) *failingDir {
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
	fileWriter func(name string) (deebee.FileWriter, error)
	mkdir      func() error
	dir        func(name string) deebee.Dir
	exists     func() (bool, error)
	listFiles  func() ([]string, error)
	deleteFile func(name string) error
}

func (d *failingDir) FileReader(name string) (io.ReadCloser, error) {
	return d.fileReader(name)
}

func (d *failingDir) FileWriter(name string) (deebee.FileWriter, error) {
	return d.fileWriter(name)
}

func (d *failingDir) Mkdir() error {
	return d.mkdir()
}

func (d *failingDir) Dir(name string) deebee.Dir {
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
