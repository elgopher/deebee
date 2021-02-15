// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package checksum

const nameLen = 8

type AlgorithmName [nameLen]byte

func encode(name AlgorithmName, sum []byte) []byte {
	algoAndSum := make([]byte, nameLen+len(sum))
	copy(algoAndSum, name[:])
	copy(algoAndSum[nameLen:], sum)
	return algoAndSum
}

func decode(algoAndSum []byte) (AlgorithmName, []byte) {
	name := AlgorithmName{}
	copy(name[:], algoAndSum[:nameLen])
	return name, algoAndSum[nameLen:]
}
