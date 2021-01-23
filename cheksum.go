package deebee

type zeroChecksum struct{}

func (n zeroChecksum) Add(b []byte) {}

func (n zeroChecksum) Calculate() uint32 {
	return 0
}
