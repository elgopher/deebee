package deebee

import (
	"io"
)

type openReader func(key string) (io.ReadCloser, error)

func openReaderFunc(dir Dir, newChecksum NewChecksum) (openReader, error) {
	return func(key string) (io.ReadCloser, error) {
		reader, err := dir.FileReader(key) // TODO error can also be returned when data is corrupted or some other IO error
		if err != nil {
			return nil, &dataNotFoundError{}
		}
		return reader, err
	}, nil
}
