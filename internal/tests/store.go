// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package tests

import (
	"errors"
	"io"

	"github.com/jacekolszak/deebee/store"
)

type StoreMock struct {
	ReturnReader        store.Reader
	ReturnReaderError   error
	ReturnVersions      []store.Version
	ReturnVersionsError error
	ReturnWriter        store.Writer
}

func (s *StoreMock) Reader(...store.ReaderOption) (store.Reader, error) {
	return s.ReturnReader, s.ReturnReaderError
}

func (s *StoreMock) Versions() ([]store.Version, error) {
	return s.ReturnVersions, s.ReturnVersionsError
}

func (s *StoreMock) Writer(...store.WriterOption) (store.Writer, error) {
	return s.ReturnWriter, nil
}

type ReaderMock struct{}

func (r *ReaderMock) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (r *ReaderMock) Close() error {
	return nil
}

func (r *ReaderMock) Version() store.Version {
	return store.Version{}
}

type ReaderFailingOnRead struct {
	ReaderMock
}

func (r *ReaderFailingOnRead) Read([]byte) (n int, err error) {
	return 0, errors.New("error")
}

type ReaderFailingOnClose struct {
	ReaderMock
}

func (r *ReaderFailingOnClose) Close() error {
	return errors.New("error")
}

type WriterMock struct {
	aborted bool
}

func (w *WriterMock) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (w *WriterMock) Close() error {
	return nil
}

func (w *WriterMock) Version() store.Version {
	return store.Version{}
}

func (w *WriterMock) AbortAndClose() {
	w.aborted = true
}

func (w *WriterMock) IsAborted() bool {
	return w.aborted
}
