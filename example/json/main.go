package main

import (
	"fmt"

	"github.com/jacekolszak/deebee/json"
	"github.com/jacekolszak/deebee/store"
)

// This example shows how to write and read JSON state.
func main() {
	s, err := store.Open("/tmp/deebee/json")
	if err != nil {
		panic(err)
	}

	in := State{
		Name: "name",
		Age:  1,
	}
	err = json.Write(s, &in)
	if err != nil {
		panic(err)
	}

	out := State{}
	err = json.Read(s, &out)
	if err != nil {
		panic(err)
	}
	fmt.Println("State read:", out)
}

type State struct {
	Name string
	Age  int
}
