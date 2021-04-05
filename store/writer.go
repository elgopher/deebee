// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"fmt"
	"hash"
	"io/ioutil"
	"os"
	"time"
)

func (s *Store) openWriter(options []WriterOption) (Writer, error) {
	opts := &WriterOptions{
		time: s.nextVersionTime(),
		sync: (*os.File).Sync,
	}
	for _, apply := range options {
		if apply == nil {
			continue
		}
		if err := apply(opts); err != nil {
			return nil, fmt.Errorf("error applying option: %w", err)
		}
	}

	name := s.dataFilename(opts.time)
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0664)
	if os.IsExist(err) {
		return nil, versionAlreadyExistsError{msg: fmt.Sprintf("version %s already exists: %s", opts.time, err)}
	}
	if err != nil {
		return nil, fmt.Errorf("error opening the file %s for writing: %w", name, err)
	}
	w := &writer{
		file:     file,
		time:     opts.time,
		sync:     opts.sync,
		checksum: newHash(),
		metrics:  &s.metrics.Write,
	}
	return w, nil
}

func (s *Store) nextVersionTime() time.Time {
	t := time.Now()
	if s.lastVersionTime == t {
		t = t.Add(time.Nanosecond)
	}
	s.lastVersionTime = t
	return t
}

type writer struct {
	file     *os.File
	time     time.Time
	sync     func(*os.File) error
	size     int64
	checksum hash.Hash

	metrics *WriteMetrics
}

func (w *writer) Write(p []byte) (int, error) {
	defer w.addElapsedTime(time.Now())

	n, err := w.file.Write(p)
	w.size += int64(n)
	w.checksum.Write(p[:n])

	w.metrics.TotalBytesWritten += n
	return n, err
}

func (w *writer) Close() error {
	defer w.addElapsedTime(time.Now())

	if err := w.writeChecksum(); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("error writing checksum: %w", err)
	}
	if err := w.sync(w.file); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("error syncing file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("error closing file: %w", err)
	}

	w.metrics.Successful++
	return nil
}

func (w *writer) writeChecksum() error {
	checksumFile := checksumFileForDataFile(w.file.Name())
	sum := w.checksum.Sum([]byte{})
	return ioutil.WriteFile(checksumFile, sum, 0664)
}

func (w *writer) Version() Version {
	return Version{
		Time: w.time,
		Size: w.size,
	}
}

func (w *writer) AbortAndClose() {
	defer w.addElapsedTime(time.Now())

	_ = w.file.Close()
	_ = os.Remove(w.file.Name())

	w.metrics.Aborted++
}

func (w *writer) addElapsedTime(start time.Time) {
	w.metrics.TotalTime += time.Since(start)
}
