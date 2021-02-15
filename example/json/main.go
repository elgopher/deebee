package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/store"
)

func main() {
	s, err := deebee.Open(tempDir())
	panicIfError(err)
	defer s.Close()

	saveState(s,
		Data{
			Foo: "1",
			Bar: "2",
		},
	)
	saveState(s,
		Data{
			Foo: "3",
			Bar: "4",
		},
	)

	data := readState(s)
	fmt.Printf("Data read from disk: %+v", data)
}

type Data struct {
	Foo string
	Bar string
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

func saveState(s *store.Store, data Data) {
	writer, err := s.Writer()
	panicIfError(err)

	err = json.NewEncoder(writer).Encode(data) // DeeBee implements standard io.Writer interface
	if err != nil {
		_ = writer.Close()
		panic(err)
	}

	err = writer.Close()
	panicIfError(err)
}

func readState(s *store.Store) Data {
	reader, err := s.Reader()
	panicIfError(err)
	defer reader.Close()

	var out Data
	err = json.NewDecoder(reader).Decode(&out) // DeeBee implements standard io.Reader interface
	panicIfError(err)
	return out
}
