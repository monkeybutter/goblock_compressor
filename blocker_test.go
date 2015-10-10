package blockcompressor

import (
	"testing"
	"encoding/binary"
	"math"
	"os"
)

func Float64bytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func create_float64_file(size int, filename string) {
	// open output file
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	for i := 0; i < size; i++ {
		file.Write(Float64bytes(float64(i)))
	}
}

func BenchmarkBlock(b *testing.B) {
	create_float64_file(1000000, "input.bin")
    	b.ResetTimer()
     	for n := 0; n < b.N; n++ {
		readerBlocks := blockGenerator("input.bin", 256, 64, 4)
		compressorBlocks := blockProcessor(blockProcessor(readerBlocks, shuff), comp)
		blockWriter(compressorBlocks, "output.bin")
	}
	b.StopTimer()
}

func BenchmarkUnblock(b *testing.B) {
    	b.ResetTimer()
     	for n := 0; n < b.N; n++ {
		readerBlocks := unblockGenerator("./output.bin", 4)
		compressorBlocks := blockProcessor(blockProcessor(readerBlocks, decomp), unshuff)
		unblockWriter(compressorBlocks, "input.bin.ini")
	}
	b.StopTimer()
}
