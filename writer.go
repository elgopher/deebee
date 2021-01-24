package deebee

import (
	"io"
)

type openWriter func(key string) (io.WriteCloser, error)

func openWriterFunc(dir Dir, newChecksum NewChecksum) openWriter {
	return func(key string) (io.WriteCloser, error) {
		return dir.FileWriter(key)
	}
}
