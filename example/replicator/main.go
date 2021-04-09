package main

import (
	"context"
	"fmt"
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

	// read latest version from replicated stores
	out := map[string]string{}
	version, err := replicator.ReadLatest(json.Decoder(out), cheapStore, sharedStore)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Version read from two stores: %+v", version)

	time.Sleep(20 * time.Second)
}
