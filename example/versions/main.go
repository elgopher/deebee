package main

import (
	"fmt"

	"github.com/jacekolszak/deebee/store"
)

// This example shows how to interact with versions
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	versions, err := s.Versions()
	if err != nil {
		panic(err)
	}

	for _, version := range versions {
		fmt.Printf("Version: %+v\n", version)
	}

	if len(versions) == 0 {
		fmt.Println("No versions found")
		return
	}

	oldest := versions[0]
	reader, err := s.Reader(store.Time(oldest.Time)) // Read state with specific time
	if err != nil {
		panic(err)
	}
	_ = reader.Close()

	err = s.DeleteVersion(oldest.Time)
	if err != nil {
		panic(err)
	}
}
