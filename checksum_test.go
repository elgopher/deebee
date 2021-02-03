package deebee_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksumIntegrityChecker(t *testing.T) {
	t.Run("should return default ChecksumIntegrityChecker", func(t *testing.T) {
		checker := deebee.ChecksumIntegrityChecker()
		assert.NotNil(t, checker)
	})

	t.Run("should return error when ChecksumIntegrityChecker is set twice", func(t *testing.T) {
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, deebee.ChecksumIntegrityChecker(), deebee.ChecksumIntegrityChecker())
		assert.Error(t, err)
		assert.Nil(t, db)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		dir := fake.ExistingDir()
		optionReturningError := func(checker *deebee.ChecksumFileIntegrityChecker) error {
			return errors.New("failed")
		}
		db, err := deebee.Open(dir, deebee.ChecksumIntegrityChecker(optionReturningError))
		assert.Error(t, err)
		assert.Nil(t, db)
	})

	t.Run("should return error error when checksum algorithm has invalid name", func(t *testing.T) {
		names := []string{"", ".", "-", " ", "A", "Z", ".7z", "a ", " 6"}
		for _, name := range names {
			t.Run(name, func(t *testing.T) {
				algorithm := invalidNameAlgorithm{name: name}
				db, err := deebee.Open(fake.ExistingDir(), deebee.ChecksumIntegrityChecker(deebee.Algorithm(algorithm)))
				assert.Error(t, err)
				assert.Nil(t, db)
			})
		}
	})

	t.Run("should accept algorithm with valid name", func(t *testing.T) {
		names := []string{"a", "z", "0", "9", "2b", "fnv128a"}
		for _, name := range names {
			t.Run(name, func(t *testing.T) {
				algorithm := invalidNameAlgorithm{name: name}
				db, err := deebee.Open(fake.ExistingDir(), deebee.ChecksumIntegrityChecker(deebee.Algorithm(algorithm)))
				require.NoError(t, err)
				assert.NotNil(t, db)
			})
		}
	})

	t.Run("should write checksum to a file with an extension having algorithm name", func(t *testing.T) {
		expectedSum := []byte{1, 2, 3, 4}
		algorithm := &fixedAlgorithm{sum: expectedSum}
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, deebee.ChecksumIntegrityChecker(deebee.Algorithm(algorithm)))
		require.NoError(t, err)
		// when
		writeData(t, db, "state", []byte("data"))
		// then
		files := filterFilesWithExtension(dir.FakeDir("state").Files(), "fixed")
		require.NotEmpty(t, files)
		assert.Equal(t, expectedSum, files[0].Data())
	})

	t.Run("should use checksum algorithm", func(t *testing.T) {
		expectedSum := []byte{1, 2, 3, 4}
		algorithm := &fixedAlgorithm{sum: expectedSum}
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, deebee.ChecksumIntegrityChecker(deebee.Algorithm(algorithm)))
		require.NoError(t, err)
		expectedData := []byte("data")
		// when
		writeData(t, db, "state", expectedData)
		actualData := readData(t, db, "state")
		// then
		assert.Equal(t, expectedData, actualData)
	})
}

func filterFilesWithExtension(files []*fake.File, extension string) []*fake.File {
	var filtered []*fake.File
	for _, file := range files {
		if strings.HasSuffix(file.Name(), "."+extension) {
			filtered = append(filtered, file)
		}
	}
	return filtered
}

type invalidNameAlgorithm struct {
	name string
}

func (i invalidNameAlgorithm) NewSum() deebee.Sum {
	return nil
}

func (i invalidNameAlgorithm) Name() string {
	return i.name
}

type fixedAlgorithm struct {
	sum []byte
}

func (c fixedAlgorithm) Name() string {
	return "fixed"
}

func (c fixedAlgorithm) NewSum() deebee.Sum {
	return &fixedSum{sum: c.sum}
}

type fixedSum struct {
	sum []byte
}

func (c *fixedSum) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (c *fixedSum) Marshal() []byte {
	return c.sum
}

func TestHashSum_Marshal(t *testing.T) {
	t.Run("should marshal sum", func(t *testing.T) {
		tests := map[string]struct {
			algorithm   deebee.ChecksumAlgorithm
			expectedSum string
		}{
			"fnv128": {
				algorithm:   deebee.Fnv128,
				expectedSum: "66ab729108757277b806e89c746322b5",
			},
			"fnv128a": {
				algorithm:   deebee.Fnv128a,
				expectedSum: "695b598c64757277b806e9704d5d6a5d",
			},
			"fixed": {
				algorithm:   &fixedAlgorithm{sum: []byte{1, 2, 3, 4}},
				expectedSum: "01020304",
			},
		}
		for name, test := range tests {

			t.Run(name, func(t *testing.T) {
				sum := test.algorithm.NewSum()
				_, err := sum.Write([]byte("data"))
				require.NoError(t, err)
				// when
				bytes := sum.Marshal()
				// then
				assert.Equal(t, test.expectedSum, fmt.Sprintf("%x", bytes))
			})
		}
	})

	t.Run("should marshal sum after two writes", func(t *testing.T) {
		tests := map[string]struct {
			algorithm   deebee.ChecksumAlgorithm
			expectedSum string
		}{
			"fnv128": {
				algorithm:   deebee.Fnv128,
				expectedSum: "66ab729108757277b806e89c746322b5",
			},
			"fnv128a": {
				algorithm:   deebee.Fnv128a,
				expectedSum: "695b598c64757277b806e9704d5d6a5d",
			},
		}
		for name, test := range tests {

			t.Run(name, func(t *testing.T) {
				sum := test.algorithm.NewSum()
				_, err := sum.Write([]byte("da"))
				require.NoError(t, err)
				_, err = sum.Write([]byte("ta"))
				require.NoError(t, err)
				// when
				bytes := sum.Marshal()
				// then
				assert.Equal(t, test.expectedSum, fmt.Sprintf("%x", bytes))
			})
		}
	})
}
