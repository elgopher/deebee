// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package json

import (
	"github.com/jacekolszak/deebee/store"
)

func Read(store ReadOnlyStore, v interface{}) error {
	return nil
}

func Write(store WriteOnlyStore, v interface{}) error {
	return nil
}

type ReadOnlyStore interface {
	Reader(...store.ReaderOption) (store.Reader, error)
}

type WriteOnlyStore interface {
	Writer(...store.WriterOption) (store.Writer, error)
}
