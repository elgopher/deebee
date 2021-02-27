package main

import (
	"fmt"
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/store"
)

func main() {
	s, err := deebee.Open(tempDir())
	panicIfError(err)
	defer s.Close()

	subscription := s.SubscribeUpdates()
	go func() {
		for {
			_, ok := <-subscription.Updates()
			if !ok {
				return
			}
			fmt.Println("Data updated")
		}
	}()
	defer subscription.Close() // Subscription is also automatically closed when Store is closed

	for i := 0; i < 10; i++ {
		saveState(s, i)
	}
}

func tempDir() string {
	dir, err := ioutil.TempDir("", "deebee")
	panicIfError(err)
	fmt.Println("Store directory:", dir)
	return dir
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func saveState(s *store.Store, data int) {
	writer, err := s.Writer()
	panicIfError(err)

	_, err = fmt.Fprintln(writer, data)
	if err != nil {
		_ = writer.Close()
		panic(err)
	}

	err = writer.Close()
	panicIfError(err)
}
