package main

import (
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/checksum"
)

// This program shows how to configure different checksum algorithm.
func main() {
	s, err := deebee.Open(tempDir(),
		checksum.IntegrityChecker(
			checksum.Algorithm(checksum.FNV128a),
		),
	)
	if err != nil {
		panic(err)
	}

	s.Close()
}

func tempDir() string {
	dir, err := ioutil.TempDir("", "deebee")
	if err != nil {
		panic(err)
	}
	return dir
}
