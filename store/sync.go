// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import "fmt"

type syncOnCloseWriter struct {
	FileWriter
}

func (s *syncOnCloseWriter) Close() error {
	if err := s.FileWriter.Sync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	if err := s.FileWriter.Close(); err != nil {
		return err
	}
	return nil
}
