package main

import (
	"context"
	"time"

	"github.com/jacekolszak/deebee/compacter"
	"github.com/jacekolszak/deebee/store"
)

// This example shows how run Compacter which removes old state versions.
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	// run compacter once
	err = compacter.RunOnce(s, compacter.MaxVersions(3))
	if err != nil {
		panic(err)
	}

	// run compacter continuously in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go compacter.Start(ctx, s)
	time.Sleep(10 * time.Second)
}
