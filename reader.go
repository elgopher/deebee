package deebee

import (
	"io"
)

type openReader func(key string) (io.ReadCloser, error)

func openReaderFunc(dir Dir, newChecksum NewChecksum) openReader {
	return func(key string) (io.ReadCloser, error) {
		dirExists, err := dir.DirExists(key)
		if err != nil {
			return nil, err
		}
		if !dirExists {
			return nil, &dataNotFoundError{}
		}
		files, err := dir.Dir(key).ListFiles()
		if err != nil {
			return nil, err
		}
		if len(files) == 0 {
			return nil, &dataNotFoundError{}
		}
		return dir.Dir(key).FileReader("data")
	}
}
