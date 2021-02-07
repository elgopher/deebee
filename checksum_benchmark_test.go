package deebee_test

import (
	"io"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/require"
)

func BenchmarkChecksumReader_Read(b *testing.B) {
	const size = 1024 * 1024 * 100

	tests := map[string]checksum.ChecksumAlgorithm{
		"crc32":   checksum.CRC32,
		"crc64":   checksum.CRC64,
		"sha512":  checksum.SHA512,
		"md5":     checksum.MD5,
		"fnv32":   checksum.FNV32,
		"fnv32a":  checksum.FNV32a,
		"fnv64":   checksum.FNV64,
		"fnv64a":  checksum.FNV64a,
		"fnv128":  checksum.FNV128,
		"fnv128a": checksum.FNV128a,
	}
	for name, algorithm := range tests {

		b.Run(name, func(b *testing.B) {
			dir := fake.ExistingDir()
			db, err := deebee.Open(dir, checksum.IntegrityChecker(checksum.Algorithm(algorithm)))
			require.NoError(b, err)
			const blockSize = 8192
			buffer := make([]byte, blockSize)
			writeBigData(b, db, size, buffer)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, err := db.Reader()
				require.NoError(b, err)
				// when
				readAll(b, reader, buffer)
			}
		})
	}
}

func writeBigData(b *testing.B, db *store.DB, fileSize int, buffer []byte) {
	writer, err := db.Writer()
	require.NoError(b, err)

	for i := 0; i < len(buffer); i++ {
		buffer[i] = byte(i)
	}
	for i := 0; i < fileSize; i += len(buffer) {
		_, err := writer.Write(buffer)
		require.NoError(b, err)
	}
	require.NoError(b, writer.Close())
}

func readAll(b *testing.B, reader io.ReadCloser, buffer []byte) {
	defer reader.Close()

	for {
		_, err := reader.Read(buffer)
		if err == io.EOF {
			return
		}
		if err != nil {
			b.Error(err)
			b.Fail()
		}
	}
}
