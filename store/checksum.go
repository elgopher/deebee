// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"hash"
	"hash/crc32"
)

func newHash() hash.Hash {
	return crc32.NewIEEE()
}
