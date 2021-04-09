// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"time"
)

func (s *Store) openReader(options []ReaderOption) (Reader, error) {
	opts := &ReaderOptions{
		chooseVersion: func(versions []Version) (Version, error) {
			return versions[len(versions)-1], nil
		},
	}

	for _, apply := range options {
		if apply == nil {
			continue
		}
		if err := apply(opts); err != nil {
			return nil, fmt.Errorf("error applying option: %w", err)
		}
	}

	versions, err := s.Versions()
	if err != nil {
		return nil, fmt.Errorf("error reading versions in directory %s: %w", s.dir, err)
	}
	if len(versions) == 0 {
		return nil, versionNotFoundError{msg: "no version found"}
	}

	version, err := opts.chooseVersion(versions)
	if err != nil {
		return nil, err
	}

	name := s.dataFilename(version.Time)
	file, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s for reading: %w", name, err)
	}
	r := &reader{
		file:     file,
		version:  version,
		checksum: newHash(),
		metrics:  &s.metrics.Read,
	}
	return r, nil
}

type ReaderOptions struct {
	chooseVersion func([]Version) (Version, error)
}

type reader struct {
	file     *os.File
	version  Version
	checksum hash.Hash

	metrics *ReadMetrics
}

func (r *reader) Read(p []byte) (int, error) {
	defer r.addElapsedTime(time.Now())

	n, err := r.file.Read(p)
	if err == io.EOF {
		sum := r.checksum.Sum([]byte{})
		expectedSum, err := r.readChecksum()
		if err != nil {
			return 0, fmt.Errorf("error reading checksum: %w", err)
		}
		if !bytes.Equal(expectedSum, sum) {
			return 0, fmt.Errorf("invalid checksum when reading file %s", r.file.Name())
		}
	}
	r.checksum.Write(p[:n])

	r.metrics.TotalBytesRead += n
	return n, err
}

func (r *reader) readChecksum() ([]byte, error) {
	checksumFile := checksumFileForDataFile(r.file.Name())
	return ioutil.ReadFile(checksumFile)
}

func (r *reader) Close() error {
	defer r.addElapsedTime(time.Now())

	if err := r.file.Close(); err != nil {
		return fmt.Errorf("error closing file: %w", err)
	}
	return nil
}

func (r *reader) Version() Version {
	return r.version
}

func (r *reader) addElapsedTime(start time.Time) {
	r.metrics.TotalTime += time.Since(start)
}
