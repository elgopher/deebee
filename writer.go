package deebee

import (
	"io"
)

type openWriter func(key string) (io.WriteCloser, error)

func openWriterFunc(dir Dir) openWriter {
	return func(key string) (io.WriteCloser, error) {
		dataDir := dir.Dir(key)
		dataDirExists, err := dataDir.Exists()
		if err != nil {
			return nil, err
		}
		if !dataDirExists {
			if err := dataDir.Mkdir(); err != nil {
				return nil, err
			}
		}
		return dataDir.FileWriter("data")
	}
}
