package fake

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/jacekolszak/deebee"
)

func ExistingDir() Dir {
	return newRootDir("existing", false)
}

func MissingDir() Dir {
	return newRootDir("missing", true)
}

func newRootDir(name string, missing bool) *dir {
	return newDir(name, missing, nil)
}

func newDir(name string, missing bool, parent *dir) *dir {
	return &dir{
		parent:      parent,
		filesByName: map[string]*File{},
		dirsByName:  map[string]*dir{},
		missing:     missing,
		name:        name,
	}
}

type Dir interface {
	deebee.Dir
	FakeDir(name string) Dir
	Files() []*File
}

type dir struct {
	parent      *dir
	filesByName map[string]*File
	dirsByName  map[string]*dir
	missing     bool
	name        string
}

func (f *dir) FileReader(name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("empty file name")
	}
	file, exists := f.filesByName[name]
	if !exists {
		return nil, fmt.Errorf("file %s does not exist", name)
	}
	return &reader{name: name, data: file.data}, nil
}

type reader struct {
	name   string
	data   bytes.Buffer
	closed bool
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, fmt.Errorf("cant read: file %s is closed", r.name)
	}
	return r.data.Read(p)
}

func (r *reader) Close() error {
	r.closed = true
	return nil
}

func (f *dir) FileWriter(name string) (deebee.FileWriter, error) {
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
	f.filesByName[name] = file
	return file, nil
}

func (f *dir) Files() []*File {
	var slice []*File
	for _, file := range f.filesByName {
		slice = append(slice, file)
	}
	return slice
}

func (f *dir) Exists() (bool, error) {
	return !f.missing, nil
}

func (f *dir) Mkdir() error {
	if f.parent != nil {
		if f.parent.missing {
			return fmt.Errorf("parent dir %s does not exist", f.parent.name)
		}
	}
	f.missing = false
	return nil
}

func (f *dir) Dir(name string) deebee.Dir {
	return f.FakeDir(name)
}

func (f *dir) FakeDir(name string) Dir {
	d, exists := f.dirsByName[name]
	if !exists {
		d = newDir(name, true, f)
		f.dirsByName[name] = d
	}
	return d
}

func (f *dir) ListFiles() ([]string, error) {
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

func (f *File) Name() string {
	return f.name
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

func (f *File) Corrupt() {
	middleOfTheFile := f.data.Len() / 2
	f.data.Bytes()[middleOfTheFile]++
}
