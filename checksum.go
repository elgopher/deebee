package deebee

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
)

type Sum interface {
	io.Writer
	Marshal() []byte
}

type FileIntegrityChecker interface {
	LatestIntegralFilename(dir Dir) (string, error)
	// Should return error on Close when checksum verification failed
	DecorateReader(reader io.ReadCloser, dir Dir, name string) io.ReadCloser
	// Should store checksum somewhere on Close.
	DecorateWriter(writer io.WriteCloser, dir Dir, name string) io.WriteCloser
}

type checksumIntegrityChecker struct {
	newSum                 func() Sum
	latestIntegralFilename func(dir Dir, newSum func() Sum) (string, error)
}

func (c *checksumIntegrityChecker) LatestIntegralFilename(dir Dir) (string, error) {
	return c.latestIntegralFilename(dir, c.newSum)
}

func (c *checksumIntegrityChecker) DecorateReader(reader io.ReadCloser, dir Dir, name string) io.ReadCloser {
	return &checksumReader{
		reader: reader,
		sum:    c.newSum(),
		name:   name,
		dir:    dir,
	}
}

func (c *checksumIntegrityChecker) DecorateWriter(writer io.WriteCloser, dir Dir, name string) io.WriteCloser {
	return &checksumWriter{
		writer: writer,
		sum:    c.newSum(),
		name:   name,
		dir:    dir,
	}
}

func lazyLatestIntegralFilename(dir Dir, newSum func() Sum) (string, error) {
	files, err := dir.ListFiles()
	if err != nil {
		return "", err
	}
	dataFiles := sortByVersionDescending(filterDatafiles(files))
	if len(dataFiles) == 0 {
		return "", &dataNotFoundError{}
	}
	for _, dataFile := range dataFiles {
		if err := verifyChecksum(dir, dataFile, newSum()); err == nil {
			return dataFile.name, nil
		}
	}
	return "", &dataNotFoundError{}
}

func verifyChecksum(dir Dir, file filename, sum Sum) error {
	fileReader, err := dir.FileReader(file.name)
	if err != nil {
		return err
	}
	reader := &checksumReader{
		reader: fileReader,
		sum:    sum,
		name:   file.name,
		dir:    dir,
	}
	if err := readAll(reader); err != nil {
		_ = reader.Close()
		return err
	}
	return reader.Close()
}

func readAll(reader io.ReadCloser) error {
	buffer := make([]byte, 4096) // FIXME reuse the buffer and make it configurable
	for {
		_, err := reader.Read(buffer)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type checksumReader struct {
	reader io.ReadCloser
	sum    Sum
	name   string
	dir    Dir
}

func (c *checksumReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if err != nil {
		return n, err
	}
	if sumBytes, sumErr := c.sum.Write(p[:n]); sumErr != nil {
		return sumBytes, fmt.Errorf("checksumReader failed: %w", sumErr)
	}
	return n, nil
}

func (c *checksumReader) Close() error {
	sumBytes := c.sum.Marshal()
	expectedSum, err := readFile(c.dir, c.name+".fnv128a")
	if err != nil {
		return err
	}
	if !bytes.Equal(sumBytes, expectedSum) {
		return fmt.Errorf("checksum mismatch for file %s", c.name)
	}
	return c.reader.Close()
}

type checksumWriter struct {
	writer io.WriteCloser
	sum    Sum
	name   string
	dir    Dir
}

func (c *checksumWriter) Write(p []byte) (n int, err error) {
	if n, err := c.sum.Write(p); err != nil {
		return n, err
	}
	return c.writer.Write(p)
}

func (c *checksumWriter) Close() error {
	sum := c.sum.Marshal()
	err := writeFile(c.dir, c.name+".fnv128a", sum)
	if err != nil {
		return err
	}
	return c.writer.Close()
}

func readFile(dir Dir, name string) ([]byte, error) {
	reader, err := dir.FileReader(name)
	if err != nil {
		return nil, err
	}
	all, err := ioutil.ReadAll(reader)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return all, nil
}

func writeFile(dir Dir, name string, payload []byte) error {
	writer, err := dir.FileWriter(name)
	if err != nil {
		return err
	}
	_, err = writer.Write(payload)
	if err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func newFnv128a() Sum {
	return &hashSum{
		Hash: fnv.New128a(),
	}
}

type hashSum struct {
	hash.Hash
}

func (f *hashSum) Marshal() []byte {
	return f.Hash.Sum([]byte{})
}
