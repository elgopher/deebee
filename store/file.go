// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"path"
	"strings"
	"time"
)

const (
	dataFileDateFormat = "2006-01-02T15_04_05.999999999Z"
	dataFileSuffix     = ".data"
	checksumFileSuffix = ".sum"
)

func (s *Store) dataFilename(t time.Time) string {
	name := t.UTC().Format(dataFileDateFormat) + dataFileSuffix
	return path.Join(s.dir, name)
}

func isDataFile(name string) bool {
	return strings.HasSuffix(name, dataFileSuffix)
}

func timeFromDataFile(name string) (time.Time, error) {
	t := name[:len(name)-len(dataFileSuffix)]
	return time.Parse(dataFileDateFormat, t)
}

func isChecksum(name string) bool {
	return strings.HasSuffix(name, checksumFileSuffix)
}

func checksumFileForDataFile(name string) string {
	return name + checksumFileSuffix
}
