package main

import (
	"github.com/jacekolszak/deebee/codec"
	"github.com/jacekolszak/deebee/store"
)

// This example shows primitive encoder writing all bytes from memory
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	bytes := []byte("Written using encoder")
	err = codec.Write(s, func(writer store.Writer) error {
		_, e := writer.Write(bytes)
		return e
	})
	if err != nil {
		panic(err)
	}
}
