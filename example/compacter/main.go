package main

import (
	"context"
	"time"

	"github.com/jacekolszak/deebee/compacter"
	"github.com/jacekolszak/deebee/json"
	"github.com/jacekolszak/deebee/store"
	"github.com/jacekolszak/yala/adapter/printer"
)

// This example shows how run Compacter which removes old state versions.
func main() {
	compacter.Logger.SetAdapter(printer.StdoutAdapter()) // enable logging in compacter go-routine

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
		if err2 := compacter.Start(ctx, s, compacter.Interval(10*time.Second)); err2 != nil {
			panic(err2)
		}
	}()
	time.Sleep(time.Minute)
}
