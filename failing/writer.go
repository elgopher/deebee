package failing

import (
	"errors"

	"github.com/jacekolszak/deebee/store"
)

type fileWriterSync struct {
	store.FileWriter
}

func (f *fileWriterSync) Sync() error {
	return errors.New("sync failed")
}
