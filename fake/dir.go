package fake

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jacekolszak/deebee/store"
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
	store.Dir
	FakeDir(name string) Dir
	Files() []*File
}

type dir struct {
	parent      *dir
	filesByName map[string]*File
	dirsByName  map[string]*dir
	missing     bool
	name        string

	mutex sync.Mutex
}

func (f *dir) FileReader(name string) (io.ReadCloser, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

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

func (f *dir) FileWriter(name string) (store.FileWriter, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

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
	f.mutex.Lock()
	defer f.mutex.Unlock()

	var slice []*File
	for _, file := range f.filesByName {
		slice = append(slice, file)
	}
	return slice
}

func (f *dir) Exists() (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return !f.missing, nil
}

func (f *dir) Mkdir() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.parent != nil {
		if f.parent.missing {
			return fmt.Errorf("parent dir %s does not exist", f.parent.name)
		}
	}
	f.missing = false
	return nil
}

func (f *dir) Dir(name string) store.Dir {
	return f.FakeDir(name)
}

func (f *dir) FakeDir(name string) Dir {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	d, exists := f.dirsByName[name]
	if !exists {
		d = newDir(name, true, f)
		f.dirsByName[name] = d
	}
	return d
}

func (f *dir) ListFiles() ([]string, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.missing {
		return nil, fmt.Errorf("dir %s does not exist", f.name)
	}
	var files []string
	for name := range f.filesByName {
		files = append(files, name)
	}
	return files, nil
}

func (f *dir) DeleteFile(name string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	_, found := f.filesByName[name]
	if !found {
		return fmt.Errorf("file %s does not exist", name)
	}
	delete(f.filesByName, name)
	return nil
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
