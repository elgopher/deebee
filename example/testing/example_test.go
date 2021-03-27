package testing

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/require"
)

func TestFunction(t *testing.T) {

	t.Skip("ignored until Store is implemented")

	t.Run("this test shows how to use Dependency Injection and temp directory to test your code", func(t *testing.T) {
		s := openStore(t) // open store in each test
		_ = Function(s)   // use dependency injection to pass a Store instance to function under test
		// some assertion goes here
	})
}

func openStore(t *testing.T, options ...store.Option) *store.Store {
	dir := createTempDir(t)

	s, err := store.Open(dir, options...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(dir) // remove temporary directory once test (and all its subtests) are done
	})
	return s
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "deebee")
	require.NoError(t, err)
	return dir
}

func Function(s *store.Store) error {
	// here goes some real production code implementation
	reader, err := s.Reader()
	if err != nil {
		return err
	}
	_, err = ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	// ...
	return nil
}
