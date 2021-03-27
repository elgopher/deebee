// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"fmt"
	"os"
)

func (s *Store) openReader(options []ReaderOption) (Reader, error) {
	opts := &ReaderOptions{
		chooseVersion: func(versions []Version) (Version, error) {
			latest := versions[len(versions)-1]
			return latest, nil
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
		file:    file,
		version: version,
	}
	return r, nil
}

type ReaderOptions struct {
	chooseVersion func([]Version) (Version, error)
}

type reader struct {
	file    *os.File
	version Version
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	//if err == io.EOF {
	//	// validate checksum. If checksum is wrong then return error
	//}
	return n, err
}

func (r *reader) Close() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("error closing file: %w", err)
	}
	return nil
}

func (r *reader) Version() Version {
	return r.version
}

type versionNotFoundError struct {
	msg string
}

func (v versionNotFoundError) Error() string {
	return v.msg
}
