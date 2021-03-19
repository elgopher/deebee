// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/failing"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/internal/storetest"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Run("should return error when dir is nil", func(t *testing.T) {
		s, err := store.Open(nil)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should open store with no options", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should skip nil option", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir, nil)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should return client error when store dir does not exist", func(t *testing.T) {
		s, err := store.Open(fake.MissingDir())
		assert.True(t, store.IsClientError(err))
		assert.Nil(t, s)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		dir := fake.ExistingDir()
		expectedError := &testError{}
		option := func(s *store.Store) error {
			return expectedError
		}
		// when
		s, err := store.Open(dir, option)
		// then
		assert.True(t, errors.Is(err, expectedError))
		assert.Nil(t, s)
	})

	t.Run("should return error when Dir.Exists() returns error", func(t *testing.T) {
		dir := failing.Exists(fake.ExistingDir())
		s, err := store.Open(dir)
		assert.Error(t, err)
		assert.Nil(t, s)
	})
}

type testError struct{}

func (e testError) Error() string {
	return "test-error"
}

func TestStore_Reader(t *testing.T) {
	t.Run("should return error when no data was previously saved", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		reader, err := s.Reader()
		// then
		assert.Nil(t, reader)
		assert.False(t, store.IsClientError(err))
		assert.True(t, store.IsDataNotFound(err))
	})

	t.Run("should return error when Store is failing", func(t *testing.T) {
		dirs := map[string]store.Dir{
			"ListFiles":  failing.ListFiles(fake.ExistingDir()),
			"FileReader": failing.FileReader(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, dir)
				if err := writeDataOrError(s, []byte("data")); err != nil {
					return
				}
				// when
				reader, err := s.Reader()
				// then
				assert.Error(t, err)
				assert.Nil(t, reader)
			})
		}
	})
}

func TestStore_Writer(t *testing.T) {
	t.Run("should return error when Store is failing", func(t *testing.T) {
		dirs := map[string]store.Dir{
			"FileWriter": failing.FileWriter(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, dir)
				// when
				writer, err := s.Writer()
				// then
				assert.Error(t, err)
				assert.Nil(t, writer)
			})
		}
	})
}

func TestReadAfterWrite(t *testing.T) {
	t.Run("should read previously written data", func(t *testing.T) {
		tests := map[string][]byte{
			"empty":      {},
			"data":       []byte("data"),
			"MB of data": makeData(1024*1024, 1),
		}
		for name, data := range tests {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, fake.ExistingDir())
				storetest.WriteData(t, s, data)
				// when
				actual := storetest.ReadData(t, s)
				// then
				assert.Equal(t, data, actual)
			})
		}
	})

	t.Run("after update should read last written data", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		updatedData := "updated"
		storetest.WriteData(t, s, []byte("data"))
		storetest.WriteData(t, s, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, s)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("after two updates should read last written data", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		updatedData := "updated"
		storetest.WriteData(t, s, []byte("data1"))
		storetest.WriteData(t, s, []byte("data2"))
		storetest.WriteData(t, s, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, s)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("should update data using different store instance", func(t *testing.T) {
		dir := fake.ExistingDir()
		s := openStore(t, dir)
		storetest.WriteData(t, s, []byte("data"))

		anotherStore := openStore(t, dir)
		updatedData := "updated"
		storetest.WriteData(t, anotherStore, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, anotherStore)
		// then
		assert.Equal(t, updatedData, string(actual))
	})
}

func TestIntegrityChecker(t *testing.T) {
	t.Run("should use custom DataIntegrityChecker", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir, store.IntegrityChecker(&failingIntegrityChecker{}))
		require.NoError(t, err)
		storetest.WriteData(t, s, []byte("data"))
		_, err = s.Reader()
		assert.Error(t, err)
	})

	tests := map[string]struct {
		OpenStore func(dir store.Dir) (*store.Store, error)
	}{
		"by default should not check data integrity at all": {
			OpenStore: func(dir store.Dir) (*store.Store, error) {
				return store.Open(dir)
			},
		},
		"NoDataIntegrityCheck should not verify data integrity": {
			OpenStore: func(dir store.Dir) (*store.Store, error) {
				return store.Open(dir, store.IntegrityChecker(failingIntegrityChecker{}), store.NoDataIntegrityCheck())
			},
		},
	}

	for name, test := range tests {

		t.Run(name, func(t *testing.T) {
			dir := fake.ExistingDir()
			s, err := test.OpenStore(dir)
			require.NoError(t, err)
			notExpected := []byte("data")
			storetest.WriteData(t, s, notExpected)
			// when
			corruptAllFiles(dir)
			// then
			data := storetest.ReadData(t, s)
			assert.NotEqual(t, notExpected, data)
		})
	}
}

func TestSync(t *testing.T) {
	data := []byte("data")

	tests := map[string]func(t *testing.T, dir store.Dir) *store.Store{
		"no checksum": func(t *testing.T, dir store.Dir) *store.Store {
			return openStore(t, dir)
		},
		"with fixed checksum": func(t *testing.T, dir store.Dir) *store.Store {
			checker := writeFixedChecksumIntegrityChecker{1, 2, 3, 4}
			return openStore(t, dir, store.IntegrityChecker(checker))
		},
	}

	for name, openStore := range tests {
		t.Run(name, func(t *testing.T) {

			t.Run("should sync all files", func(t *testing.T) {
				dir := fake.ExistingDir()
				s := openStore(t, dir)
				// when
				storetest.WriteData(t, s, data)
				// then
				for _, file := range dir.Files() {
					assert.Equal(t, file.Data(), file.SyncedData(), "no all data synced")
				}
			})

			t.Run("should return error when sync failed", func(t *testing.T) {
				dir := failing.FileWriterSync(fake.ExistingDir())
				s := openStore(t, dir)
				writer, err := s.Writer()
				require.NoError(t, err)
				_, err = writer.Write(data)
				require.NoError(t, err)
				// when
				err = writer.Close()
				// then
				assert.Error(t, err)
			})
		})
	}
}

func TestStore_Versions(t *testing.T) {
	t.Run("should return empty state versions", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("should return one state version", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		storetest.WriteData(t, s, []byte("data"))
		// when
		states, err := s.Versions()
		require.NoError(t, err)
		require.Len(t, states, 1)
	})

	t.Run("should return two state versions", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		storetest.WriteData(t, s, []byte("data"))
		storetest.WriteData(t, s, []byte("updated"))
		// when
		states, err := s.Versions()
		require.NoError(t, err)
		require.Len(t, states, 2)
		assert.True(t, states[0].Revision() != states[1].Revision(), "revisions are not different")
	})

	t.Run("should return sorted states by revision", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		const revisions = 256
		for i := 0; i < revisions; i++ {
			storetest.WriteData(t, s, []byte("data"))
		}
		// when
		states, err := s.Versions()
		require.NoError(t, err)
		require.Len(t, states, revisions)
		for i := 0; i < revisions-1; i++ {
			assert.True(t, states[i].Revision() < states[i+1].Revision(), "revisions are not sorted: states[%d].Revision < states[%d].Revision", i, i+1)
		}
	})

	t.Run("should return time of state creation", func(t *testing.T) {
		creationTime, err := time.Parse(time.RFC3339, "1999-01-01T12:00:00Z")
		require.NoError(t, err)
		currentTime, err := time.Parse(time.RFC3339, "2077-01-01T12:00:00Z")
		require.NoError(t, err)

		fakeTime := &fakeNow{currentTime: creationTime}
		s := openStoreWithOptions(t, store.Now(fakeTime.Now))
		storetest.WriteData(t, s, []byte("data"))
		// when
		fakeTime.currentTime = currentTime
		states, err := s.Versions()
		// then
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, creationTime, states[0].Time())
	})
}

func TestState_Remove(t *testing.T) {
	t.Run("should return empty states when last remaining version is removed", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		storetest.WriteData(t, s, []byte("data"))
		states, err := s.Versions()
		require.NoError(t, err)
		// when
		err = states[0].Remove()
		// then
		require.NoError(t, err)
		states, err = s.Versions()
		require.NoError(t, err)
		assert.Empty(t, states)
	})

	t.Run("should remove one state's version when two versions are available", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		storetest.WriteData(t, s, []byte("data1"))
		storetest.WriteData(t, s, []byte("data2"))
		states, err := s.Versions()
		require.NoError(t, err)
		removedState := states[0]
		// when
		err = removedState.Remove()
		// then
		require.NoError(t, err)
		states, err = s.Versions()
		require.NoError(t, err)
		assert.Len(t, states, 1)
		assert.NotEqual(t, removedState.Revision(), states[0].Revision(), "wrong revision removed")
	})

	t.Run("should return error when dir.DeleteFile is failing", func(t *testing.T) {
		dir := failing.DeleteFile(fake.ExistingDir())
		s := openStore(t, dir)

		storetest.WriteData(t, s, []byte("data1"))
		states, err := s.Versions()
		require.NoError(t, err)
		// when
		err = states[0].Remove()
		// then
		assert.Error(t, err)
	})
}

type writeFixedChecksumIntegrityChecker []byte

func (f writeFixedChecksumIntegrityChecker) DecorateReader(reader io.ReadCloser, _ store.ReadChecksum) (io.ReadCloser, error) {
	return reader, nil
}

func (f writeFixedChecksumIntegrityChecker) DecorateWriter(writer io.WriteCloser, writeChecksum store.WriteChecksum) (io.WriteCloser, error) {
	return &writeFixedChecksumWriter{
		checksum:      f,
		writeChecksum: writeChecksum,
		WriteCloser:   writer,
	}, nil
}

type writeFixedChecksumWriter struct {
	checksum []byte
	io.WriteCloser
	writeChecksum store.WriteChecksum
}

func (w writeFixedChecksumWriter) Close() error {
	if err := w.writeChecksum(w.checksum); err != nil {
		return err
	}
	return w.WriteCloser.Close()
}

type failingIntegrityChecker struct{}

func (f failingIntegrityChecker) DecorateReader(reader io.ReadCloser, readChecksum store.ReadChecksum) (io.ReadCloser, error) {
	return &failingReader{ReadCloser: reader}, nil
}

type failingReader struct {
	io.ReadCloser
}

func (f failingReader) Close() error {
	return errors.New("error")
}

func (f failingIntegrityChecker) DecorateWriter(writer io.WriteCloser, writeChecksum store.WriteChecksum) (io.WriteCloser, error) {
	return writer, nil
}

func openStore(t *testing.T, dir store.Dir, options ...store.Option) *store.Store {
	s, err := store.Open(dir, options...)
	require.NoError(t, err)
	return s
}

func writeDataOrError(s *store.Store, data []byte) error {
	writer, err := s.Writer()
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	if err != nil {
		_ = writer.Close()
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func makeData(size int, fillWith byte) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = fillWith
	}
	return data
}

func corruptAllFiles(dir fake.Dir) {
	files := dir.Files()
	for _, file := range files {
		file.Corrupt()
	}
}
