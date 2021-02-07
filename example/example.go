package main

import (
	"fmt"
	"io/ioutil"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/os"
	"github.com/jacekolszak/deebee/store"
)

func main() {
	dir := os.Dir(tempDir())
	fmt.Println("Store directory:", dir)

	s, err := deebee.Open(dir)
	panicIfError(err)

	saveState(s, "Some very long data :)")
	saveState(s, "Updated data even longer than before :)")
	data := readState(s)
	fmt.Println("Data read from disk:", data)
}

func saveState(s *store.Store, data string) {
	writer, err := s.Writer()
	panicIfError(err)

	_, err = writer.Write([]byte(data))
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

	data, err := ioutil.ReadAll(reader)
	panicIfError(err)
	return string(data)
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func tempDir() string {
	dir, err := ioutil.TempDir("", "deebee")
	panicIfError(err)
	return dir
}
