// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"fmt"
	"io/ioutil"
	"time"
)

func (s *Store) versions() ([]Version, error) {
	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading dir %s failed: %w", s.dir, err)
	}

	var versions []Version
	for _, file := range files {
		t, err := time.Parse(filenameDateFormat, file.Name())
		if err != nil {
			return nil, fmt.Errorf("parsing filename %s failed: %w", file, err)
		}
		v := Version{
			Time: t,
			Size: file.Size(),
		}
		versions = append(versions, v)
	}
	return versions, nil
}
