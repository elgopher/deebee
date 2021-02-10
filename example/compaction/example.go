package main

import (
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/compaction"
	"github.com/jacekolszak/deebee/os"
)

func main() {
	dir := os.Dir(tempDir())

	s, err := deebee.Open(dir,
		compaction.Strategy(
			compaction.KeepLatestVersions(2),
			compaction.MaxVersions(10),
		))
	if err != nil {
		panic(err)
	}
	defer s.Close()
}

func tempDir() string {
	dir, err := ioutil.TempDir("", "deebee")
	if err != nil {
		panic(err)
	}
	return dir
}
