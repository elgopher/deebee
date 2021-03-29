// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
)

func (s *Store) openReader(options []ReaderOption) (Reader, error) {
	opts := &ReaderOptions{
		chooseVersion: func(versions []Version) (Version, error) {
			for i := len(versions) - 1; i >= 0; i-- {
				latest := versions[i]
				integral, err := s.isVersionIntegral(latest)
				if err != nil {
					return Version{}, err
				}
				if integral {
					return latest, nil
				}
				if !integral && i > 0 {
					log.Printf("Version \"%s\" stored in dir \"%s\" is corrupted. Falling back to previous one.", latest.Time, s.dir)
				}
			}
			return Version{}, versionNotFoundError{msg: "no data found: all versions corrupted"}
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
}

func (r *reader) Read(p []byte) (int, error) {
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
	return n, err
}

func (r *reader) readChecksum() ([]byte, error) {
	checksumFile := checksumFileForDataFile(r.file.Name())
	return ioutil.ReadFile(checksumFile)
}

func (s *Store) isVersionIntegral(v Version) (bool, error) {
	dataFile := s.dataFilename(v.Time)
	checksumFile := checksumFileForDataFile(dataFile)
	expectedChecksum, err := ioutil.ReadFile(checksumFile)
	if err != nil {
		return false, err
	}
	checksum, err := calculateChecksum(dataFile)
	if err != nil {
		return false, err
	}
	return bytes.Equal(expectedChecksum, checksum), nil
}

func calculateChecksum(filename string) ([]byte, error) {
	checksum := newHash()
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := f.Read(buffer)
		if err == io.EOF {
			return checksum.Sum([]byte{}), nil
		}
		if err != nil {
			return nil, err
		}
		checksum.Write(buffer[:n])
	}
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
