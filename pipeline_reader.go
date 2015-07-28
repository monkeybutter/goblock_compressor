package main

import (
	"io"
	"os"
	"bytes"
	"github.com/golang/snappy/snappy"
)

const kB = 1024

type block struct {
	n   int
	buf []byte
}

func block_generator(filePath string, block_size int) <-chan block {

	out := make(chan block, 4)

	go func() {

		// open input file
		file, err := os.Open(filePath)

		if err != nil {
			panic(err)
		}

		// close fi on exit and check for its returned error
		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()

		// define block size
		buf := make([]byte, block_size)

		for {
			// read a block
			n, err := file.Read(buf)

			if err != nil && err != io.EOF {
				panic(err)
			}

			if n == 0 {
				break
			}

			out <- block{n, buf[:n]}

		}
		close(out)
	}()

	return out
}

func _shuffle_part_n(chunk, part []byte, n, size int) {

        for i:=0; i<len(part); i++ {
              part[i] = chunk[i*size+n]
        }
}

func block_shuffler(in <-chan block, block_size, size int) <-chan block {

	out := make(chan block)

	go func() {
		
		for ablock := range in {

			if ablock.n == block_size {
				
				shuffled_buf := make([]byte, ablock.n)

				for i:=0; i<ablock.n/size; i++ {	
					part := shuffled_buf[size*i:size*(i+1)]
					_shuffle_part_n(ablock.buf, part, i, size)
	        	}

	        	out <- block{ablock.n, shuffled_buf}
	        	
			} else {
				out <- ablock
			}
		}

		close(out)
	}()

	return out
}

func block_compressor(in <-chan block, block_size int) <-chan block {

	out := make(chan block)

	go func() {

		var buf bytes.Buffer
		
		for ablock := range in {

			if ablock.n == block_size {
				buf.Reset()
				writer := snappy.NewWriter(&buf)
				writer.Write(ablock.buf[:ablock.n])

				out <- block{len(buf.Bytes()), buf.Bytes()}
			} else {
				out <- ablock
			}
		}

		close(out)
	}()

	return out
}

func block_writer(in <-chan block, filePath string) bool {

	// open output file
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	for block := range in {
		file.Write(block.buf[:block.n])
	}

	return true
}

func main() {
	block_size := 16*kB

	// nothing
	block_writer(
		block_generator("input.txt", block_size),
	"output0.txt")

	// shuffling
	block_writer(
		block_shuffler(
			block_generator("input.txt", block_size),
		block_size, 4), 
	"output1.txt")

	// compression
	block_writer(
		block_compressor(
			block_generator("input.txt", block_size),
		block_size), 
	"output2.txt")

	// shuffling + compression
	block_writer(
		block_compressor(
			block_shuffler(
				block_generator("input.txt", block_size),
			block_size, 4), 
		block_size), 
	"output3.txt")


}
