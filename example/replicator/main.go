package main

import (
	"context"
	"time"

	"github.com/jacekolszak/deebee/json"
	"github.com/jacekolszak/deebee/replicator"
	"github.com/jacekolszak/deebee/store"
)

func main() {
	cheapStore, err := store.Open("/tmp/deebee/cheap")
	if err != nil {
		panic(err)
	}

	err = json.Write(cheapStore, map[string]string{})
	if err != nil {
		panic(err)
	}

	sharedStore, err := store.Open("/tmp/deebee/shared") // shared store can use NFS, AWS EFS etc.
	if err != nil {
		panic(err)
	}

	// copy recent version once
	err = replicator.CopyFromTo(cheapStore, sharedStore)
	if err != nil {
		panic(err)
	}

	// copy recent versions continuously in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err2 := replicator.StartFromTo(ctx, cheapStore, sharedStore, replicator.Interval(10*time.Second)); err2 != nil {
			panic(err2)
		}
	}()

	time.Sleep(20 * time.Second)
}
