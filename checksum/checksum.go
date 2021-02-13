package checksum

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/jacekolszak/deebee/store"
)

func IntegrityChecker(options ...IntegrityCheckerOption) store.Option {
	return func(s *store.Store) error {
		checker := &DataIntegrityChecker{
			algorithm: CRC32,
		}
		for _, apply := range options {
			if err := apply(checker); err != nil {
				return fmt.Errorf("error applying IntegrityChecker option: %w", err)
			}
		}
		return store.IntegrityChecker(checker)(s)
	}
}

type IntegrityCheckerOption func(*DataIntegrityChecker) error

func Algorithm(algorithm ChecksumAlgorithm) IntegrityCheckerOption {
	return func(checker *DataIntegrityChecker) error {
		checker.algorithm = algorithm
		return nil
	}
}

type ChecksumAlgorithm interface {
	NewSum() Sum
	Name() AlgorithmName
}

type Sum interface {
	io.Writer
	Marshal() []byte
}

type DataIntegrityChecker struct {
	algorithm ChecksumAlgorithm
}

func (c *DataIntegrityChecker) DecorateReader(reader io.ReadCloser, readChecksum store.ReadChecksum) (io.ReadCloser, error) {
	algoAndSum, err := readChecksum()
	if err != nil {
		return nil, fmt.Errorf("reading checksum failed: %w", err)
	}
	name, expectedSum := decode(algoAndSum)
	sum := c.sumFromAlgorithmNameOrDefault(name)

	return &checksumReader{
		reader:      reader,
		sum:         sum,
		expectedSum: expectedSum,
	}, nil
}

func (c *DataIntegrityChecker) sumFromAlgorithmNameOrDefault(name AlgorithmName) Sum {
	var sum Sum
	if algo, found := algorithmsByName[name]; found {
		sum = algo.NewSum()
	} else {
		sum = c.algorithm.NewSum()
	}
	return sum
}

func (c *DataIntegrityChecker) DecorateWriter(writer io.WriteCloser, writeChecksum store.WriteChecksum) (io.WriteCloser, error) {
	return &checksumWriter{
		writer:        writer,
		sum:           c.algorithm.NewSum(),
		algorithmName: c.algorithm.Name(),
		writeChecksum: writeChecksum,
	}, nil
}

type checksumReader struct {
	reader      io.ReadCloser
	sum         Sum
	expectedSum []byte
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
	if !bytes.Equal(sumBytes, c.expectedSum) {
		return errors.New("checksum mismatch")
	}
	return c.reader.Close()
}

type checksumWriter struct {
	writer        io.WriteCloser
	sum           Sum
	algorithmName AlgorithmName
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
	algoAndSum := encode(c.algorithmName, sum)
	err := c.writeChecksum(algoAndSum)
	if err != nil {
		_ = c.writer.Close()
		return err
	}
	return c.writer.Close()
}
