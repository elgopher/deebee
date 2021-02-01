package deebee

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"io/ioutil"
)

func ChecksumIntegrityChecker(options ...ChecksumIntegrityCheckerOption) Option {
	return func(db *DB) error {
		checker := &checksumIntegrityChecker{
			algorithm:              Fnv128a,
			latestIntegralFilename: lazyLatestIntegralFilename,
		}
		for _, apply := range options {
			if err := apply(checker); err != nil {
				return fmt.Errorf("error applying ChecksumIntegrityChecker option: %w", err)
			}
		}

		if err := db.setFileIntegrityChecker(checker); err != nil {
			return err
		}
		return nil
	}
}

type ChecksumIntegrityCheckerOption func(*checksumIntegrityChecker) error

func Algorithm(algorithm ChecksumAlgorithm) ChecksumIntegrityCheckerOption {
	return func(checker *checksumIntegrityChecker) error {
		checker.algorithm = algorithm
		return nil
	}
}

type ChecksumAlgorithm interface {
	NewSum() Sum
	FileExtension() string
}

type Sum interface {
	io.Writer
	Marshal() []byte
}

type checksumIntegrityChecker struct {
	algorithm              ChecksumAlgorithm
	latestIntegralFilename func(dir Dir, algorithm ChecksumAlgorithm) (string, error)
}

func (c *checksumIntegrityChecker) LatestIntegralFilename(dir Dir) (string, error) {
	return c.latestIntegralFilename(dir, c.algorithm)
}

func (c *checksumIntegrityChecker) DecorateReader(reader io.ReadCloser, dir Dir, name string) io.ReadCloser {
	return &checksumReader{
		reader: reader,
		sum:    c.algorithm.NewSum(),
		name:   name,
		dir:    dir,
	}
}

func (c *checksumIntegrityChecker) DecorateWriter(writer io.WriteCloser, dir Dir, name string) io.WriteCloser {
	return &checksumWriter{
		writer:           writer,
		sum:              c.algorithm.NewSum(),
		name:             name,
		checksumFilename: name + "." + c.algorithm.FileExtension(),
		dir:              dir,
	}
}

func lazyLatestIntegralFilename(dir Dir, algorithm ChecksumAlgorithm) (string, error) {
	files, err := dir.ListFiles()
	if err != nil {
		return "", err
	}
	dataFiles := sortByVersionDescending(filterDatafiles(files))
	if len(dataFiles) == 0 {
		return "", &dataNotFoundError{}
	}
	for _, dataFile := range dataFiles {
		if err := verifyChecksum(dir, dataFile, algorithm.NewSum()); err == nil {
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
	writer           io.WriteCloser
	sum              Sum
	name             string
	checksumFilename string
	dir              Dir
}

func (c *checksumWriter) Write(p []byte) (n int, err error) {
	if n, err := c.sum.Write(p); err != nil {
		return n, err
	}
	return c.writer.Write(p)
}

func (c *checksumWriter) Close() error {
	sum := c.sum.Marshal()
	err := writeFile(c.dir, c.checksumFilename, sum)
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

var Fnv128a = &hashAlgorithm{
	newSum: func() Sum {
		return &HashSum{
			Hash: fnv.New128a(),
		}
	},
	fileExtension: "fnv128a",
}

type hashAlgorithm struct {
	newSum        func() Sum
	fileExtension string
}

func (h *hashAlgorithm) FileExtension() string {
	return h.fileExtension
}

func (h *hashAlgorithm) NewSum() Sum {
	return h.newSum()
}

type HashSum struct {
	hash.Hash
}

func (f *HashSum) Marshal() []byte {
	return f.Hash.Sum([]byte{})
}
