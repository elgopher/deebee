// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"fmt"
	"io/fs"
	"io/ioutil"
)

func (s *Store) versions() ([]Version, error) {
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading dir %s failed: %w", s.dir, err)
	}

	checksums := checksumSet(files)

	var versions []Version
	for _, file := range files {
		filename := file.Name()
		if isDataFile(filename) {
			_, hasChecksum := checksums[checksumFileForDataFile(filename)]
			if !hasChecksum {
				continue
			}
			t, err := timeFromDataFile(filename)
			if err != nil {
				return nil, fmt.Errorf("parsing filename %s failed: %w", file, err)
			}
			v := Version{
				Time: t,
				Size: file.Size(),
			}
			versions = append(versions, v)
		}
	}
	return versions, nil
}

func checksumSet(files []fs.FileInfo) map[string]struct{} {
	checksums := map[string]struct{}{}
	for _, file := range files {
		if isChecksum(file.Name()) {
			checksums[file.Name()] = struct{}{}
		}
	}
	return checksums
}
