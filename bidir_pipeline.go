package main

import (
	"io"
	"fmt"
	"os"
	"github.com/golang/snappy/snappy"
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

func block_generator(filePath string, block_size int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, 1), make(chan Block, 1)} 

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

		// Block ready to hold reading
		out.Upstream <- Block{make([]byte, block_size), 0}

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

func shuffle_part_n(block, part []byte, n, type_size int) {

		fmt.Println(len(block), (len(part)-1)*type_size+n)

        for i:=0; i<len(part); i++ {
              part[i] = block[i*type_size+n]
        }
}

func block_shuffler(in DuplexPipe, block_size, type_size int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, 1), make(chan Block, 1)} 

	// Block ready to hold reading
	out.Upstream <- Block{make([]byte, block_size), 0}

	var shuffled_buf Block

	go func() {	

		//var buf bytes.Buffer
		for block := range in.Downstream {

			shuffled_buf = <- out.Upstream

			if block.N == block_size {

				parts := block_size/type_size

				for i:=0; i<type_size; i++ {
					part := shuffled_buf.Buf[parts*i:parts*(i+1)]
					shuffle_part_n(block.Buf, part, i, type_size)
        		}

				shuffled_buf.N = block_size
		
				
			} else {
				shuffled_buf.N = block.N
				copy(shuffled_buf.Buf[:shuffled_buf.N], block.Buf)
				
			}

			in.Upstream <- block
			out.Downstream <- shuffled_buf
		}
		close(out.Downstream)
	}()

	return out
}


func block_compressor(in DuplexPipe, block_size int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan Block, 1), make(chan Block, 1)} 

	// Block ready to hold reading
	out.Upstream <- Block{make([]byte, block_size), 0}

	var comp_buf Block

	go func() {	

		//var buf bytes.Buffer
		for block := range in.Downstream {

			comp_buf = <- out.Upstream

			if block.N == block_size {


				// We are allocating comp_chunk extra here
				// ! Look snappy code examples to do this better
				comp_chunk, err := snappy.Encode(comp_buf.Buf, block.Buf)
				if err != nil {
					panic(err)
				}
				
				// !we have to copy this extra chunk into our shared slice
				// this misses the point of having reusable slices... :-(
				comp_buf.N = len(comp_chunk)
				copy(comp_buf.Buf[:comp_buf.N], comp_chunk)

		
				
			} else {
				comp_buf.N = block.N
				copy(comp_buf.Buf[:comp_buf.N], block.Buf)
				
			}

			in.Upstream <- block
			out.Downstream <- comp_buf
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
		fmt.Println(len(block.Buf), block.N)
		file.Write(block.Buf[:block.N])
		in.Upstream <- block
	}

	return true
}

func main() {
	block_size := 1*kB

	// read & write
	//block_writer(block_generator("input.txt", block_size), "output.txt")
	// read compress write
	//block_writer(block_compressor(block_generator("input.txt", block_size), block_size), "output.txt")

	// read shuffle compress write
	block_writer(block_compressor(block_shuffler(block_generator("input.txt", block_size), block_size, 4), block_size), "output.txt")

}