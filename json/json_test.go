// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package json_test

import (
	"testing"

	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/json"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	t.Run("should write json", func(t *testing.T) {
		s := tests.OpenStore(t)
		v := State{Field: "value"}
		// when
		err := json.Write(s, v)
		// then
		require.NoError(t, err)
		data := tests.ReadData(t, s)
		assert.JSONEq(t, `{"Field":"value"}`, string(data))
	})

	t.Run("should abort writing json on marshaling error", func(t *testing.T) {
		s := tests.OpenStore(t)
		v := InvalidState{}
		// when
		err := json.Write(s, v)
		// then
		assert.Error(t, err)
		// and
		_, err = s.Reader()
		assert.True(t, store.IsVersionNotFound(err))
	})
}

func TestRead(t *testing.T) {
	t.Run("should read json", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte(`{"Field":"value"}`))
		v := State{}
		// when
		err := json.Read(s, &v)
		// then
		require.NoError(t, err)
		assert.Equal(t, State{Field: "value"}, v)
	})

	t.Run("should return error on unmarshalling error", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte(`{}`))
		// when
		err := json.Read(s, nil)
		// then
		assert.Error(t, err)
	})

}

type State struct {
	Field string
}

type InvalidState struct {
	Filed chan string
}
