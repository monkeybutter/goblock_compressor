package main

import (
	"github.com/golang/snappy"
	"fmt"
	"os"
	"io"
)

const kB = 1024

type Block struct {
	Buf []byte
	N   int
	BlockID   int
}

type DuplexPipe struct {
	Downstream chan Block
	Upstream   chan Block
}


func block_generator(filePath string, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, conc_level), make(chan Block, conc_level)}
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- Block{make([]byte, block_size), 0, 0}
	}
	

	go func() {

		file, err := os.Open(filePath)

		if err != nil {
			panic(err)
		}

		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()

		var buf Block

		i := 0

		for {
			buf = <-out.Upstream

			// read a block
			n, err := file.Read(buf.Buf)
			buf.N = n
			buf.BlockID = i

			fmt.Println("Reading block", buf.N, "ID", buf.BlockID)

			if err != nil && err != io.EOF {
				panic(err)
			}

			if n == 0 {
				break
			}

			out.Downstream <- buf

			i++

		}
		close(out.Downstream)
	}()

	return out
}


func block_compressor(in DuplexPipe, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, conc_level), make(chan Block, conc_level)}
	// Block ready to hold reading
	comp_len := snappy.MaxEncodedLen(block_size)
	for i := 0; i < conc_level; i++ {
		out.Upstream <- Block{make([]byte, comp_len), 0, 0}
	}

	var comp_buf Block

	go func() {

		done := make(chan bool, conc_level)

		for block := range in.Downstream {
			comp_buf = <-out.Upstream
			done <- false

			go func() {

				fmt.Println("Compressing block", block.N, "ID", block.BlockID)

				if block.N == block_size {

					// We are allocating comp_chunk extra here to know length
					// ! Fork snappy to return len(comp_buf.Buf) instead of
					// the the actual slice of comp_buf.Buf
					comp_chunk := snappy.Encode(comp_buf.Buf, block.Buf)

					// this misses the point of having reusable slices... :-(
					comp_buf.N = len(comp_chunk)
					comp_buf.BlockID = block.BlockID

				} else {
					comp_buf.N = block.N
					comp_buf.BlockID = block.BlockID
					copy(comp_buf.Buf[:comp_buf.N], block.Buf)
				}

				in.Upstream <- block
				out.Downstream <- comp_buf

				<- done
			}()
		}
		// Wait for them to finish
		for i := 0; i < conc_level; i++ {
			done <- true
		}
		close(out.Downstream)
	}()

	return out
}


func block_writer(in DuplexPipe, filePath string) bool {

	// open output file
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	fmt.Println("Here comes the header")

	for block := range in.Downstream {
		file.Write(block.Buf[:block.N])
		fmt.Println("Writing block", block.N, "ID", block.BlockID)
		in.Upstream <- block
	}

	return true
}


func main() {

	fileName := "npy1e7.bin"
	block_size := 512 * kB

	block_writer(
		block_compressor(
			block_generator(fileName, block_size, 4), 
		block_size, 4),
	"output.bin")

}
