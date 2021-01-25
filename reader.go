package deebee

import (
	"io"
)

type openReader func(key string) (io.ReadCloser, error)

func openReaderFunc(dir Dir, newChecksum NewChecksum) openReader {
	return func(key string) (io.ReadCloser, error) {
		dataDir := dir.Dir(key)
		dataDirExists, err := dataDir.Exists()
		if err != nil {
			return nil, err
		}
		if !dataDirExists {
			return nil, &dataNotFoundError{}
		}
		files, err := dataDir.ListFiles()
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			return nil, &dataNotFoundError{}
		}
		return dataDir.FileReader("data")
	}
}
