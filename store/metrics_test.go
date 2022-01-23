// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"testing"

	"github.com/elgopher/deebee/internal/tests"
	"github.com/elgopher/deebee/store"
	"github.com/stretchr/testify/assert"
)

func TestStore_Metrics(t *testing.T) {

	t.Run("should return copy of metrics", func(t *testing.T) {
		s := tests.OpenStore(t)
		metrics := s.Metrics()
		metrics.Write.Aborted = 1
		assert.NotEqual(t, metrics, s.Metrics())
	})

	t.Run("should change metrics after successful write", func(t *testing.T) {
		s := tests.OpenStore(t)
		data := []byte("data")
		tests.WriteData(t, s, data)
		// when
		metrics := s.Metrics().Write
		// then
		assert.Equal(t,
			store.WriteMetrics{
				WriterCalls:       1,
				Successful:        1,
				Aborted:           0,
				TotalBytesWritten: len(data),
				TotalTime:         metrics.TotalTime,
			},
			metrics)
	})

	t.Run("should update metrics after aborted write", func(t *testing.T) {
		s := tests.OpenStore(t)
		data := []byte("partial data")
		writer, _ := s.Writer()
		_, _ = writer.Write(data)
		writer.AbortAndClose()
		// when
		metrics := s.Metrics().Write
		// then
		assert.Equal(t,
			store.WriteMetrics{
				WriterCalls:       1,
				Successful:        0,
				Aborted:           1,
				TotalBytesWritten: len(data),
				TotalTime:         metrics.TotalTime,
			},
			metrics)
	})

	t.Run("should change metrics after executing Reader", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("data"))
		r, _ := s.Reader()
		_, _ = r.Read(make([]byte, 1))
		defer closeSilently(r)
		// when
		metrics := s.Metrics().Read
		// then
		assert.Equal(t,
			store.ReadMetrics{
				ReaderCalls:    1,
				TotalBytesRead: 1,
				TotalTime:      metrics.TotalTime,
			},
			metrics)
	})
}
