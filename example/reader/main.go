package main

import (
	"fmt"
	"io"

	"github.com/jacekolszak/deebee/store"
)

// This example shows how to read a recent version of state
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	reader, err := s.Reader()
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	bytes, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	fmt.Println("Bytes read", bytes)
	fmt.Printf("Version %+v", reader.Version())
}
