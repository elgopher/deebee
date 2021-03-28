// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package json

import (
	"encoding/json"

	"github.com/jacekolszak/deebee/store"
)

func Read(store ReadOnlyStore, v interface{}, options ...store.ReaderOption) error {
	reader, err := store.Reader(options...)
	if err != nil {
		return err
	}
	err = json.NewDecoder(reader).Decode(v)
	if err != nil {
		_ = reader.Close()
		return err
	}
	return reader.Close()
}

func Write(store WriteOnlyStore, v interface{}, options ...store.WriterOption) error {
	writer, err := store.Writer(options...)
	if err != nil {
		return err
	}
	err = json.NewEncoder(writer).Encode(v)
	if err != nil {
		writer.AbortAndClose()
		return err
	}
	return writer.Close()
}

type ReadOnlyStore interface {
	Reader(...store.ReaderOption) (store.Reader, error)
}

type WriteOnlyStore interface {
	Writer(...store.WriterOption) (store.Writer, error)
}
