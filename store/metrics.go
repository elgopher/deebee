// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import "time"

type Metrics struct {
	Read  ReadMetrics
	Write WriteMetrics
}

type ReadMetrics struct {
	ReaderCalls    int // Number of Store.Reader() calls
	TotalBytesRead int
	TotalTime      time.Duration
}

type WriteMetrics struct {
	WriterCalls       int // Number of Store.Writer() calls
	Successful        int // Number of successful writes (when writer was closed without aborting)
	Aborted           int // Number of aborted writes (when Writer.AbortAndClose was called)
	TotalBytesWritten int
	TotalTime         time.Duration
}
