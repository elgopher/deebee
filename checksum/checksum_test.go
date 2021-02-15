// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package checksum_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/internal/storetest"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrityChecker(t *testing.T) {
	t.Run("should return default IntegrityChecker option", func(t *testing.T) {
		checker := checksum.IntegrityChecker()
		assert.NotNil(t, checker)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		dir := fake.ExistingDir()
		optionReturningError := func(checker *checksum.DataIntegrityChecker) error {
			return errors.New("failed")
		}
		s, err := store.Open(dir, checksum.IntegrityChecker(optionReturningError))
		assert.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should use checksum algorithm", func(t *testing.T) {
		expectedSum := []byte{1, 2, 3, 4}
		algorithm := &fixedAlgorithm{sum: expectedSum}
		dir := fake.ExistingDir()
		s, err := store.Open(dir, checksum.IntegrityChecker(checksum.Algorithm(algorithm)))
		require.NoError(t, err)
		expectedData := []byte("data")
		// when
		storetest.WriteData(t, s, expectedData)
		actualData := storetest.ReadData(t, s)
		// then
		assert.Equal(t, expectedData, actualData)
	})
}

func TestReadAfterWrite(t *testing.T) {
	t.Run("should return data not found error when all files are corrupted", func(t *testing.T) {
		tests := map[string]int{
			"no updates": 0,
			"one update": 1,
		}
		for name, numberOfUpdates := range tests {

			t.Run(name, func(t *testing.T) {
				dir := fake.ExistingDir()
				s := openStoreWithChecksumIntegrityChecker(t, dir)
				storetest.WriteData(t, s, []byte("new"))

				for i := 0; i < numberOfUpdates; i++ {
					storetest.WriteData(t, s, []byte("update"))
				}
				// when
				corruptAllFiles(dir)
				reader, err := s.Reader()
				// then
				require.Error(t, err)
				assert.True(t, store.IsDataNotFound(err))
				assert.Nil(t, reader)
			})
		}
	})

	t.Run("should return error when file was integral during verification, but became corrupted during read", func(t *testing.T) {
		dir := fake.ExistingDir()
		s := openStoreWithChecksumIntegrityChecker(t, dir)
		storetest.WriteData(t, s, []byte("data"))
		reader, err := s.Reader()
		require.NoError(t, err)
		corruptAllFiles(dir)
		_, err = ioutil.ReadAll(reader)
		require.NoError(t, err)
		// when
		err = reader.Close()
		// then
		assert.Error(t, err)
	})

	t.Run("should return last not corrupted data", func(t *testing.T) {
		dir := fake.ExistingDir()
		s := openStoreWithChecksumIntegrityChecker(t, dir)
		storetest.WriteData(t, s, []byte("old"))
		storetest.WriteData(t, s, []byte("new"))
		// when
		corruptLastAddedFile(dir)
		reader, err := s.Reader()
		// then
		require.NoError(t, err)
		assert.NotNil(t, reader)
	})

	t.Run("during read should use algorithm used at the time of writing", func(t *testing.T) {
		algorithms := []checksum.ChecksumAlgorithm{
			checksum.MD5,
			checksum.CRC32,
			checksum.CRC64,
			checksum.SHA512,
			checksum.FNV64a,
			checksum.FNV64,
			checksum.FNV32,
			checksum.FNV32a,
			checksum.FNV128,
			checksum.FNV128a,
		}
		for _, algorithm := range algorithms {
			name := algorithm.Name()

			t.Run(string(name[:]), func(t *testing.T) {
				dir := fake.ExistingDir()
				storeWithOldAlgorithm, err := store.Open(dir, checksum.IntegrityChecker(checksum.Algorithm(algorithm)))
				require.NoError(t, err)
				data := []byte("data")
				storetest.WriteData(t, storeWithOldAlgorithm, data)
				require.NoError(t, storeWithOldAlgorithm.Close())
				//
				storeWithNewAlgorithm, err := store.Open(dir, checksum.IntegrityChecker(checksum.Algorithm(fixedAlgorithm{})))
				require.NoError(t, err)
				actualData := storetest.ReadData(t, storeWithNewAlgorithm)
				// then
				assert.Equal(t, data, actualData)
			})
		}
	})
}

func openStoreWithChecksumIntegrityChecker(t *testing.T, dir store.Dir) *store.Store {
	s, err := store.Open(dir, checksum.IntegrityChecker())
	require.NoError(t, err)
	return s
}

type fixedAlgorithm struct {
	sum []byte
}

func (c fixedAlgorithm) Name() checksum.AlgorithmName {
	return [8]byte{1}
}

func (c fixedAlgorithm) NewSum() checksum.Sum {
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
	tests := map[string]struct {
		algorithm   checksum.ChecksumAlgorithm
		expectedSum string
	}{
		"crc32": {
			algorithm:   checksum.CRC32,
			expectedSum: "adf3f363",
		},
		"crc64": {
			algorithm:   checksum.CRC64,
			expectedSum: "3408641350000000",
		},
		"sha512": {
			algorithm:   checksum.SHA512,
			expectedSum: "77c7ce9a5d86bb386d443bb96390faa120633158699c8844c30b13ab0bf92760b7e4416aea397db91b4ac0e5dd56b8ef7e4b066162ab1fdc088319ce6defc876",
		},
		"md5": {
			algorithm:   checksum.MD5,
			expectedSum: "8d777f385d3dfec8815d20f7496026dc",
		},
		"fnv32": {
			algorithm:   checksum.FNV32,
			expectedSum: "74cb23bd",
		},
		"fnv32a": {
			algorithm:   checksum.FNV32a,
			expectedSum: "d872e2a5",
		},
		"fnv64": {
			algorithm:   checksum.FNV64,
			expectedSum: "14dfb87eecce7a1d",
		},
		"fnv64a": {
			algorithm:   checksum.FNV64a,
			expectedSum: "855b556730a34a05",
		},
		"fnv128": {
			algorithm:   checksum.FNV128,
			expectedSum: "66ab729108757277b806e89c746322b5",
		},
		"fnv128a": {
			algorithm:   checksum.FNV128a,
			expectedSum: "695b598c64757277b806e9704d5d6a5d",
		},
		"fixed": {
			algorithm:   &fixedAlgorithm{sum: []byte{1, 2, 3, 4}},
			expectedSum: "01020304",
		},
	}

	t.Run("should marshal sum", func(t *testing.T) {
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

func corruptAllFiles(dir fake.Dir) {
	files := dir.Files()
	for _, file := range files {
		file.Corrupt()
	}
}

func corruptLastAddedFile(dir fake.Dir) {
	files := dir.Files()
	files[len(files)-1].Corrupt()
}
