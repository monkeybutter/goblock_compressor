package blockcompressor

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
)

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

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

func peep_float64(start int64, iters int, filename string) {
	file, err := os.Open(filename)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	file.Seek(start, 0)
	number := make([]byte, 8)

	for i := 0; i < iters; i++ {
		file.Read(number)
		fmt.Println(number, Float64frombytes(number)+.5)
	}
}

func main() {
	file_size, _ := strconv.Atoi(os.Args[1])
	create_float64_file(file_size, os.Args[2])
	//peep_float64(0, 10, os.Args[1])
}
