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

	saveState(s, "Some very long data :)")
	saveState(s, "Updated data even longer than before :)")

	data := readState(s)
	fmt.Println("Data read from disk:", data)
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

func saveState(s *store.Store, data string) {
	writer, err := s.Writer()
	panicIfError(err)

	_, err = fmt.Fprintln(writer, data) // DeeBee implements standard io.Writer interface
	if err != nil {
		_ = writer.Close()
		panic(err)
	}

	err = writer.Close()
	panicIfError(err)
}

func readState(s *store.Store) string {
	reader, err := s.Reader()
	panicIfError(err)
	defer reader.Close()

	data, err := ioutil.ReadAll(reader) // DeeBee implements standard io.Reader interface
	panicIfError(err)
	return string(data)
}
