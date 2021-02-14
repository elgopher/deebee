package main

import (
	"fmt"
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/compaction"
)

// This program shows how to change compaction strategy.
func main() {
	s, err := deebee.Open(tempDir(),
		compaction.Strategy(
			compaction.KeepLatestVersions(2),
			compaction.MaxVersions(10),
		))
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
	fmt.Println("Store directory:", dir)
	return dir
}
