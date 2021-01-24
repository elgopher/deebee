package deebee

import (
	"io"
)

type openWriter func(key string) (io.WriteCloser, error)

func openWriterFunc(dir Dir, newChecksum NewChecksum) openWriter {
	return func(key string) (io.WriteCloser, error) {
		exists, err := dir.DirExists(key)
		if err != nil {
			return nil, err
		}
		if !exists {
			if err := dir.Mkdir(key); err != nil {
				return nil, err
			}
		}
		return dir.Dir(key).FileWriter("data")
	}
}
