// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package codec

import (
	"errors"

	"github.com/jacekolszak/deebee/store"
)

func Read(s ReadOnlyStore, decoder Decoder, options ...store.ReaderOption) (store.Version, error) {
	if decoder == nil {
		return store.Version{}, errors.New("nil decoder")
	}
	reader, err := s.Reader(options...)
	if err != nil {
		return store.Version{}, err
	}
	err = decoder(reader)
	if err != nil {
		_ = reader.Close()
		return store.Version{}, err
	}
	err = reader.Close()
	if err != nil {
		return store.Version{}, err
	}
	return reader.Version(), nil
}

type Decoder func(reader store.Reader) error

func Write(s WriteOnlyStore, encoder Encoder, options ...store.WriterOption) error {
	if encoder == nil {
		return errors.New("nil encoder")
	}
	writer, err := s.Writer(options...)
	if err != nil {
		return err
	}
	err = encoder(writer)
	if err != nil {
		writer.AbortAndClose()
		return err
	}
	return writer.Close()
}

type Encoder func(writer store.Writer) error

// ReadLatest reads latest version or fallback to previous one when decoder returned error
func ReadLatest(s ReadOnlyStore, decoder Decoder) (store.Version, error) {
	emptyVersion := store.Version{}
	if decoder == nil {
		return emptyVersion, errors.New("nil decoder")
	}
	if s == nil {
		return emptyVersion, errors.New("nil store")
	}
	versions, err := s.Versions()
	if err != nil {
		return emptyVersion, store.NewVersionNotFoundErrorWithCause("listing versions failed", err)
	}
	if len(versions) == 0 {
		return emptyVersion, store.NewVersionNotFoundError("empty store")
	}
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		_, err = Read(s, decoder, store.Time(version.Time))
		if err == nil {
			return version, nil
		}
	}
	return emptyVersion, store.NewVersionNotFoundError("no version can be decoded")
}

type ReadOnlyStore interface {
	Versions() ([]store.Version, error)
	Reader(...store.ReaderOption) (store.Reader, error)
}

type WriteOnlyStore interface {
	Writer(...store.WriterOption) (store.Writer, error)
}
