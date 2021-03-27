// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"fmt"
	"os"
	"path"
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
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		return nil, fmt.Errorf("error opening the file %s for writing: %w", name, err)
	}
	w := &writer{
		file: file,
		time: opts.time,
		sync: opts.sync,
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

const filenameDateFormat = "2006-01-02T15_04_05.999999999Z"

func (s *Store) dataFilename(t time.Time) string {
	return path.Join(s.dir, t.UTC().Format(filenameDateFormat))
}

type writer struct {
	file *os.File
	time time.Time
	sync func(*os.File) error
	size int64
}

func (w *writer) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *writer) Close() error {
	if err := w.sync(w.file); err != nil {
		return fmt.Errorf("error syncing file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("error closing file: %w", err)
	}
	return nil
}

func (w *writer) Version() Version {
	return Version{
		Time: w.time,
		Size: w.size,
	}
}

func (w *writer) AbortAndClose() {
	_ = w.file.Close()
	_ = os.Remove(w.file.Name())
}
