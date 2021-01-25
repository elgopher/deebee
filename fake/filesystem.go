package fake

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/jacekolszak/deebee"
)

type Dir struct {
	parent      *Dir
	filesByName map[string]*File
	dirs        map[string]*Dir
	missing     bool
	name        string
}

func (f *Dir) FileReader(name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	file, exists := f.filesByName[name]
	if !exists {
		return nil, fmt.Errorf("file %s does not exist", name)
	}
	file.closed = false
	return file, nil
}

func (f *Dir) FileWriter(name string) (deebee.FileWriter, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	_, exists := f.filesByName[name]
	if exists {
		return nil, fmt.Errorf("file %s already exists", name)
	}
	file := &File{
		name: name,
	}
	f.addFile(name, file)
	return file, nil
}

func (f *Dir) addFile(name string, file *File) {
	if f.filesByName == nil {
		f.filesByName = map[string]*File{}
	}
	f.filesByName[name] = file
}

func (f *Dir) Files() []*File {
	var slice []*File
	for _, file := range f.filesByName {
		slice = append(slice, file)
	}
	return slice
}

func (f *Dir) Exists() (bool, error) {
	return !f.missing, nil
}

func (f *Dir) Mkdir() error {
	if f.parent != nil {
		if f.parent.missing {
			return fmt.Errorf("parent dir %s does not exist", f.parent.name)
		}
	}
	f.missing = false
	return nil
}

func (f *Dir) Dir(name string) deebee.Dir {
	dir, exists := f.dirs[name]
	if !exists {
		dir = &Dir{
			parent:  f,
			missing: true,
			name:    name,
		}
		f.addDir(name, dir)
	}
	return dir
}

func (f *Dir) addDir(name string, dir *Dir) {
	if f.dirs == nil {
		f.dirs = map[string]*Dir{}
	}
	f.dirs[name] = dir
}

func (f *Dir) ListFiles() ([]string, error) {
	if f.missing {
		return nil, fmt.Errorf("dir %s does not exist", f.name)
	}
	var files []string
	for name := range f.filesByName {
		files = append(files, name)
	}
	return files, nil
}

type File struct {
	data        bytes.Buffer
	syncedBytes int
	name        string
	closed      bool
}

func (f *File) Empty() bool {
	return f.data.Len() == 0
}

func (f *File) Closed() bool {
	return f.closed
}

func (f *File) Data() []byte {
	return f.data.Bytes()
}

func (f *File) Write(p []byte) (n int, err error) {
	if f.closed {
		return 0, fmt.Errorf("cant write: file %s is closed", f.name)
	}
	return f.data.Write(p)
}

func (f *File) Sync() error {
	f.syncedBytes = f.data.Len()
	return nil
}

func (f *File) SyncedData() []byte {
	return f.data.Bytes()[:f.syncedBytes]
}

func (f *File) Close() error {
	f.closed = true
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	if f.closed {
		return 0, fmt.Errorf("cant read: file %s is closed", f.name)
	}
	return f.data.Read(p)
}
