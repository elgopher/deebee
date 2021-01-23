package fake

import "github.com/jacekolszak/deebee"

func NewChecksum() deebee.Checksum {
	return &Checksum{}
}

type Checksum struct {
	sum uint32
}

func (f *Checksum) Add(bytes []byte) {
	for _, b := range bytes {
		f.sum += uint32(b)
	}
}

func (f *Checksum) Calculate() uint32 {
	return f.sum
}
