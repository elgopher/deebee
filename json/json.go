// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package json

import (
	"encoding/json"
	"io"

	"github.com/elgopher/deebee/codec"
	"github.com/elgopher/deebee/store"
)

func Read(s codec.ReadOnlyStore, out interface{}, options ...store.ReaderOption) (store.Version, error) {
	return codec.Read(s, Decoder(out), options...)
}

func Decoder(out interface{}) codec.Decoder {
	return func(reader io.Reader) error {
		return json.NewDecoder(reader).Decode(out)
	}
}

func Write(s codec.WriteOnlyStore, in interface{}, options ...store.WriterOption) error {
	return codec.Write(s, Encoder(in), options...)
}

func Encoder(in interface{}) codec.Encoder {
	return func(writer io.Writer) error {
		return json.NewEncoder(writer).Encode(in)
	}
}
