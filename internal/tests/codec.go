package tests

import (
	"io"

	"github.com/jacekolszak/deebee/store"
)

type FakeDecoder struct {
	dataRead []byte
}

func (f *FakeDecoder) Decode(reader store.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	f.dataRead = data
	return nil
}

func (f *FakeDecoder) DataRead() []byte {
	return f.dataRead
}
