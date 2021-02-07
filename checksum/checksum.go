package checksum

import (
	"bytes"
	"fmt"
	"io"
	"regexp"

	"github.com/jacekolszak/deebee/store"
)

func IntegrityChecker(options ...IntegrityCheckerOption) store.Option {
	return func(db *store.DB) error {
		checker := &DataIntegrityChecker{
			algorithm: CRC32,
		}
		for _, apply := range options {
			if err := apply(checker); err != nil {
				return fmt.Errorf("error applying IntegrityChecker option: %w", err)
			}
		}
		return store.IntegrityChecker(checker)(db)
	}
}

type IntegrityCheckerOption func(*DataIntegrityChecker) error

var algorithmNameRegex = regexp.MustCompile("^[a-z0-9]+$")

func Algorithm(algorithm ChecksumAlgorithm) IntegrityCheckerOption {
	return func(checker *DataIntegrityChecker) error {
		if !algorithmNameRegex.MatchString(algorithm.Name()) {
			return fmt.Errorf("invalid algorithm name: %s", algorithm.Name())
		}
		checker.algorithm = algorithm
		return nil
	}
}

type ChecksumAlgorithm interface {
	NewSum() Sum
	// Name must be digits and/or lower-case alphabetical characters
	Name() string
}

type Sum interface {
	io.Writer
	Marshal() []byte
}

type DataIntegrityChecker struct {
	algorithm ChecksumAlgorithm
}

func (c *DataIntegrityChecker) DecorateReader(reader io.ReadCloser, name string, readChecksum store.ReadChecksum) io.ReadCloser {
	return &checksumReader{
		reader:       reader,
		sum:          c.algorithm.NewSum(),
		name:         name,
		algorithm:    c.algorithm.Name(),
		readChecksum: readChecksum,
	}
}

func (c *DataIntegrityChecker) DecorateWriter(writer io.WriteCloser, name string, writeChecksum store.WriteChecksum) io.WriteCloser {
	return &checksumWriter{
		writer:        writer,
		sum:           c.algorithm.NewSum(),
		name:          name,
		algorithm:     c.algorithm.Name(),
		writeChecksum: writeChecksum,
	}
}

type checksumReader struct {
	reader       io.ReadCloser
	name         string
	sum          Sum
	algorithm    string
	readChecksum store.ReadChecksum
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
	expectedSum, err := c.readChecksum(c.algorithm)
	if err != nil {
		return err
	}
	if !bytes.Equal(sumBytes, expectedSum) {
		return fmt.Errorf("checksum mismatch for file %s", c.name)
	}
	return c.reader.Close()
}

type checksumWriter struct {
	writer        io.WriteCloser
	name          string
	sum           Sum
	algorithm     string
	writeChecksum store.WriteChecksum
}

func (c *checksumWriter) Write(p []byte) (n int, err error) {
	if n, err := c.sum.Write(p); err != nil {
		return n, err
	}
	return c.writer.Write(p)
}

func (c *checksumWriter) Close() error {
	sum := c.sum.Marshal()
	err := c.writeChecksum(c.algorithm, sum)
	if err != nil {
		_ = c.writer.Close()
		return err
	}
	return c.writer.Close()
}
