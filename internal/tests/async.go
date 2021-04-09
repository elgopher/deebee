// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func RunAsync(f func()) Async {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	return Async{done: done}
}

type Async struct {
	done <-chan struct{}
}

func (a *Async) WaitOrFailAfter(t *testing.T, timeout time.Duration) {
	select {
	case <-a.done:
	case <-time.After(timeout):
		assert.FailNow(t, "timeout waiting for async function to finish")
	}
}
