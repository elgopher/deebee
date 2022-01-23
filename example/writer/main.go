package main

import (
	"fmt"

	"github.com/elgopher/deebee/store"
)

// This example shows how to update state using Writer
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	writer, err := s.Writer()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Saving version %+v", writer.Version())

	_, err = writer.Write([]byte("Hello, world"))
	if err != nil {
		writer.Close()
		panic(err)
	}

	err = writer.Close()
	if err != nil {
		panic(err)
	}
}
