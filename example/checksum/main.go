package main

import (
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/os"
)

// This program shows how to configure different checksum algorithm.
func main() {
	dir := os.Dir(tempDir())

	s, err := deebee.Open(dir,
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
