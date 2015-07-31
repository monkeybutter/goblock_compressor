package main

import (
	"io"
	"os"
	"runtime"
	"github.com/golang/snappy"
)

const kB = 1024

type Block struct {
	Buf []byte
	N   int
}

type DuplexPipe struct {
	Downstream chan Block
	Upstream chan Block
}

func block_generator(filePath string, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, conc_level), make(chan Block, conc_level)} 
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- Block{make([]byte, block_size), 0}
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

		for {
			buf = <- out.Upstream

			// read a block
			n, err := file.Read(buf.Buf)
			buf.N = n

			if err != nil && err != io.EOF {
				panic(err)
			}

			if n == 0 {
				break
			}

			out.Downstream <- buf

		}
		close(out.Downstream)
	}()

	return out
}

func shuffle_part_n(chunk, part []byte, n, type_size int) {

        
}

func block_shuffler(in DuplexPipe, block_size, type_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, conc_level), make(chan Block, conc_level)} 
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- Block{make([]byte, block_size), 0}
	}

	var shuff_buf Block

	go func() {	

		done := make(chan bool, conc_level)

		for block := range in.Downstream {
			shuff_buf = <- out.Upstream
			done <- false

			go func() {	
	
				if block.N == block_size {

					for i:=0; i<block_size/type_size; i++ {
						part := shuff_buf.Buf[type_size*i:type_size*(i+1)]

						for j:=0; j<len(part); j++ {
						      part[j] = block.Buf[j*type_size+i]
						}
			        }

					shuff_buf.N = block.N

					
				} else {
					shuff_buf.N = block.N
					copy(shuff_buf.Buf[:shuff_buf.N], block.Buf)
				}

				in.Upstream <- block
				out.Downstream <- shuff_buf	

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

func block_compressor(in DuplexPipe, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, conc_level), make(chan Block, conc_level)} 
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- Block{make([]byte, block_size), 0}
	}

	var comp_buf Block

	go func() {	

		done := make(chan bool, conc_level)

		for block := range in.Downstream {
			comp_buf = <- out.Upstream
			done <- false

			go func() {	
	
				if block.N == block_size {

					// We are allocating comp_chunk extra here to know length
					// ! Fork snappy to return len(comp_buf.Buf) instead of 
					// the the actual slice of comp_buf.Buf
					comp_chunk := snappy.Encode(comp_buf.Buf, block.Buf)
					
					// this misses the point of having reusable slices... :-(
					comp_buf.N = len(comp_chunk)

					
				} else {
					comp_buf.N = block.N
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

	for block := range in.Downstream {
		file.Write(block.Buf[:block.N])
		in.Upstream <- block
	}

	return true
}

func main() {
	runtime.GOMAXPROCS(4)
	block_size := 256*kB

	// read & write
	//block_writer(block_generator("input.bin", block_size), "output.bin")
	// read compress write
	//block_writer(block_compressor(block_generator("npy5e8.bin", block_size, 4), block_size, 4), "output.bin")
	// read shuffle compress write
	block_writer(block_compressor(block_shuffler(block_generator("npy5e8.bin", block_size, 4), block_size, 4, 4), block_size, 4), "output2.bin")

}
