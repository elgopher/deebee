package main

import (
	"context"
	"time"

	"github.com/jacekolszak/deebee/compacter"
	"github.com/jacekolszak/deebee/json"
	"github.com/jacekolszak/deebee/store"
)

// This example shows how run Compacter which removes old state versions.
func main() {
	s, err := store.Open("/tmp/deebee")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		err = json.Write(s, map[string]string{})
		if err != nil {
			panic(err)
		}
	}

	// run compacter once
	err = compacter.RunOnce(s)
	if err != nil {
		panic(err)
	}

	// run compacter continuously in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err2 := compacter.Start(ctx, s); err2 != nil {
			panic(err2)
		}
	}()
	time.Sleep(10 * time.Second)
}
