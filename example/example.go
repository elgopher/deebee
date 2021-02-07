package main

import (
	"fmt"
	"io/ioutil"

	"github.com/jacekolszak/deebee/os"
	"github.com/jacekolszak/deebee/store"
)

func main() {
	dir := os.Dir(tempDir())
	fmt.Println("Database directory:", dir)

	db, err := store.Open(dir)
	panicIfError(err)

	saveState(db, "Some very long data :)")
	saveState(db, "Updated data even longer than before :)")
	data := readState(db)
	fmt.Println("Data read from disk:", data)
}

func saveState(db *store.DB, data string) {
	writer, err := db.Writer()
	panicIfError(err)

	_, err = writer.Write([]byte(data))
	if err != nil {
		_ = writer.Close()
		panic(err)
	}

	err = writer.Close()
	panicIfError(err)
}

func readState(db *store.DB) string {
	reader, err := db.Reader()
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
