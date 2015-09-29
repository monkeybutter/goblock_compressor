package main

import (
	"fmt"
	"os"
	"encoding/binary"
	"github.com/golang/snappy"
)

const kB = 1024

func main() {
	// open input file
	dataFile, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := dataFile.Close(); err != nil {
			panic(err)
		}
	}()

	// open uncompressed file
	outFile, err := os.Create(os.Args[1] + ".ini")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := outFile.Close(); err != nil {
			panic(err)
		}
	}()

	/*
	// Create uncompressed file
	outFile, err := os.Create(os.Args[1] + ".ini")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := outFile.Close(); err != nil {
			panic(err)
		}
	}()*/

	f32s := make([]byte, 4)
	bytes := make([]byte, 1)

	dataFile.Seek(3, 0)
	dataFile.Read(bytes)
	type_size := int(bytes[0])

	dataFile.Seek(8, 0)
	dataFile.Read(f32s)
	file_size := binary.LittleEndian.Uint32(f32s)

	dataFile.Seek(12, 0)
	dataFile.Read(f32s)
	block_size := binary.LittleEndian.Uint32(f32s)

	nBlocks := (file_size/(256*kB)) + 1

	fmt.Println(type_size, file_size, block_size, nBlocks)

	offsets := make([]byte, nBlocks*8)
	dataFile.Seek(20, 0)
	dataFile.Read(offsets)

	dbuf := make([]byte, snappy.MaxEncodedLen(int(block_size)))

	for i := uint32(0); i<nBlocks; i++ {
		start := binary.LittleEndian.Uint32(offsets[i*8:(i*8)+4])
		size := binary.LittleEndian.Uint32(offsets[i*8+4:(i*8)+8])
		block := make([]byte, size)
		dataFile.ReadAt(block, int64(start))
		d, _ := snappy.Decode(dbuf, block)
		/*
		bufo := make([]byte, len(d))

		for ts := 0; ts < type_size; ts++ {
			for bs := 0; bs < int(block_size)/64; bs++ {
				bufo[bs*type_size+ts] = d[bs+(ts*int(block_size)/type_size)]
			}
		}*/

		outFile.Write(d)
	}

	fmt.Println("Start", binary.LittleEndian.Uint32(offsets[len(offsets)-8:len(offsets)-4]))
	fmt.Println("Size", binary.LittleEndian.Uint32(offsets[len(offsets)-4:len(offsets)]))

	/*
	dataFile.Read(f32s)
	fmt.Println("Next number", binary.LittleEndian.Uint32(f32s))
	*/
}
