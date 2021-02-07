package checksum

import (
	"crypto/md5"
	"crypto/sha512"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
)

var CRC64 = &algorithm{
	newSum: func() Sum {
		table := crc64.MakeTable(crc64.ISO)
		return &hashSum{
			Hash: crc64.New(table),
		}
	},
	name: "crc64",
}

var CRC32 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: crc32.New(crc32.IEEETable),
		}
	},
	name: "crc32",
}

var SHA512 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: sha512.New(),
		}
	},
	name: "sha512",
}

var MD5 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: md5.New(),
		}
	},
	name: "md5",
}

var FNV32 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New32(),
		}
	},
	name: "fnv32",
}

var FNV32a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New32a(),
		}
	},
	name: "fnv32a",
}

var FNV64 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New64(),
		}
	},
	name: "fnv64",
}

var FNV64a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New64a(),
		}
	},
	name: "fnv64a",
}

var FNV128 = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New128(),
		}
	},
	name: "fnv128",
}

var FNV128a = &algorithm{
	newSum: func() Sum {
		return &hashSum{
			Hash: fnv.New128a(),
		}
	},
	name: "fnv128a",
}

type algorithm struct {
	newSum func() Sum
	name   string
}

func (h *algorithm) Name() string {
	return h.name
}

func (h *algorithm) NewSum() Sum {
	return h.newSum()
}

type hashSum struct {
	hash.Hash
}

func (f *hashSum) Marshal() []byte {
	return f.Hash.Sum([]byte{})
}
