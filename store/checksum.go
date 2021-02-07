package store

import (
	"bytes"
	"crypto/md5"
	"crypto/sha512"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"io"
	"regexp"
)

func ChecksumIntegrityChecker(options ...ChecksumIntegrityCheckerOption) Option {
	return func(db *DB) error {
		checker := &ChecksumDataIntegrityChecker{
			algorithm: CRC32,
		}
		for _, apply := range options {
			if err := apply(checker); err != nil {
				return fmt.Errorf("error applying ChecksumIntegrityChecker option: %w", err)
			}
		}
		if err := db.setDataIntegrityChecker(checker); err != nil {
			return err
		}
		return nil
	}
}

type ChecksumIntegrityCheckerOption func(*ChecksumDataIntegrityChecker) error

var algorithmNameRegex = regexp.MustCompile("^[a-z0-9]+$")

func Algorithm(algorithm ChecksumAlgorithm) ChecksumIntegrityCheckerOption {
	return func(checker *ChecksumDataIntegrityChecker) error {
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

type ChecksumDataIntegrityChecker struct {
	algorithm ChecksumAlgorithm
}

func (c *ChecksumDataIntegrityChecker) DecorateReader(reader io.ReadCloser, name string, readChecksum ReadChecksum) io.ReadCloser {
	return &checksumReader{
		reader:       reader,
		sum:          c.algorithm.NewSum(),
		name:         name,
		algorithm:    c.algorithm.Name(),
		readChecksum: readChecksum,
	}
}

func (c *ChecksumDataIntegrityChecker) DecorateWriter(writer io.WriteCloser, name string, writeChecksum WriteChecksum) io.WriteCloser {
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
	readChecksum ReadChecksum
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
	writeChecksum WriteChecksum
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

var CRC64 = &algorithm{
	newSum: func() Sum {
		table := crc64.MakeTable(crc64.ISO)
		return &hashSum{
			Hash: crc64.New(table),
		}
	},
	name: "crc64",
}

var CRC32 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: crc32.New(crc32.IEEETable),
		}
	},
	name: "crc32",
}

var SHA512 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: sha512.New(),
		}
	},
	name: "sha512",
}

var MD5 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: md5.New(),
		}
	},
	name: "md5",
}

var FNV32 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New32(),
		}
	},
	name: "fnv32",
}

var FNV32a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New32a(),
		}
	},
	name: "fnv32a",
}

var FNV64 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New64(),
		}
	},
	name: "fnv64",
}

var FNV64a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New64a(),
		}
	},
	name: "fnv64a",
}

var FNV128 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New128(),
		}
	},
	name: "fnv128",
}

var FNV128a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New128a(),
		}
	},
	name: "fnv128a",
}

type algorithm struct {
	newSum func() Sum
	name   string
}

func (h *algorithm) Name() string {
	return h.name
}

func (h *algorithm) NewSum() Sum {
	return h.newSum()
}

type hashSum struct {
	hash.Hash
}

func (f *hashSum) Marshal() []byte {
	return f.Hash.Sum([]byte{})
}
