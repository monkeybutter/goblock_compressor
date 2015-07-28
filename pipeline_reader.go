package main

import (
	"bytes"
	"github.com/golang/snappy"
	"io"
	"os"
)

const kB = 1024

func block_generator(filePath string, block_size int) <-chan []byte {
	out := make(chan []byte)

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

			bufc := make([]byte, n)
			copy(bufc, buf[:n])
			out <- bufc

		}
		close(out)
	}()

	return out
}

func _shuffle_part_n(chunk, part []byte, n, size int) {

	for i := 0; i < len(part); i++ {
		part[i] = chunk[i*size+n]
	}
}

func block_shuffler(in <-chan []byte, block_size, size int) <-chan []byte {

	out := make(chan []byte)

	go func() {

		for block := range in {

			if len(block) == block_size {

				shuffled_buf := make([]byte, block_size)

				for i := 0; i < block_size/size; i++ {
					part := shuffled_buf[size*i : size*(i+1)]
					_shuffle_part_n(block, part, i, size)
				}

				out <- shuffled_buf

			} else {
				out <- block
			}
		}

		close(out)
	}()

	return out
}

func block_compressor(in <-chan []byte, block_size int) <-chan []byte {

	out := make(chan []byte)

	go func() {

		var buf bytes.Buffer

		for block := range in {

			if len(block) == block_size {
				buf.Reset()
				writer := snappy.NewWriter(&buf)
				writer.Write(block)

				out <- buf.Bytes()
			} else {
				out <- block
			}
		}

		close(out)
	}()

	return out
}

func block_writer(in <-chan []byte, filePath string) bool {

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
		file.Write(block)
	}

	return true
}

func main() {
	block_size := 512 * kB
	/*
	// nothing
	block_writer(
		block_generator("output.bin", block_size),
		"output0.txt")

	// shuffling
	block_writer(
		block_shuffler(
			block_generator("output.bin", block_size),
			block_size, 4),
		"output1.txt")

	// compression
	block_writer(
		block_compressor(
			block_generator("output.bin", block_size),
			block_size),
		"output2.txt")
	*/
	// shuffling + compression
	block_writer(
		block_compressor(
			block_shuffler(
				block_generator("output.bin", block_size),
				block_size, 4),
			block_size),
		"output3.txt")

}
