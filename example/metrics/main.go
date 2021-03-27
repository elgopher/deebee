package main

import (
	"fmt"

	"github.com/jacekolszak/deebee/store"
)

// This example shows how to get Store metrics
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	metrics := s.Metrics()
	fmt.Printf("%+v", metrics)
}
