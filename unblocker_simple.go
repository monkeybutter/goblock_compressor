package main

import (
	"encoding/gob"
	"fmt"
	"github.com/golang/snappy"
	"os"
	"strconv"
)

const kB = 1024

type Offset struct {
	Start int
	Size  int
}

func main() {

	data := make(map[int]Offset)

	// open data file
	mapFile, err := os.Open(os.Args[1] + ".gob")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mapDecoder := gob.NewDecoder(mapFile)
	err = mapDecoder.Decode(&data)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mapFile.Close()

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

	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])
	dbuf := make([]byte, snappy.MaxEncodedLen(block_size))

	for i := 0; ; i++ {
		if val, ok := data[i]; ok {

			buf := make([]byte, val.Size)
			dataFile.ReadAt(buf, int64(val.Start))

			if i < len(data)-1 {
				d, _ := snappy.Decode(dbuf, buf)

				bufo := make([]byte, len(d))

				for j := 0; j < type_size; j++ {
					for i := 0; i < block_size/64; i++ {
						bufo[i*type_size+j] = d[i+(j*block_size/type_size)]
					}
				}

				outFile.Write(bufo)
			} else {
				outFile.Write(buf)
			}

		} else {
			break
		}
	}

}
