package main

import (
	"encoding/gob"
	"github.com/golang/snappy"
	"io"
	"os"
	"strconv"
	"sync"
)

const kB = 1024

type Block struct {
	Buf     []byte
	NBytes  int
	BlockID int
}

type Offset struct {
	Start int
	Size  int
}

type DuplexPipe struct {
	Downstream chan *Block
	Upstream   chan *Block
}

func block_generator(filePath string, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, block_size), 0, 0}
	}

	go func() {

		var buf *Block

		file, err := os.Open(filePath)

		if err != nil {
			panic(err)
		}

		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()

		for i := 0; ; i++ {
			buf = <-out.Upstream

			// create a block
			n, err := file.Read(buf.Buf)
			buf.NBytes = n
			buf.BlockID = i

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

func shuff(shuff_buf, block *Block, in, out DuplexPipe, block_size, type_size int, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.NBytes == block_size {

		shuff_unit := block_size / type_size

		for i := 0; i < type_size; i++ {
			shuff_offset := i * shuff_unit

			for j := 0; j < shuff_unit; j++ {
				shuff_buf.Buf[shuff_offset+j] = block.Buf[j*type_size+i]
			}
		}

		shuff_buf.NBytes = block.NBytes
		shuff_buf.BlockID = block.BlockID

	} else {
		shuff_buf.NBytes = block.NBytes
		shuff_buf.BlockID = block.BlockID
		copy(shuff_buf.Buf[:shuff_buf.NBytes], block.Buf)
	}

	in.Upstream <- block
	out.Downstream <- shuff_buf
}

func block_shuffler(in DuplexPipe, block_size, type_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}

	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, block_size), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go shuff(<-out.Upstream, block, in, out, block_size, type_size, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return out
}

func comp(comp_buf, block *Block, in, out DuplexPipe, block_size int, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.NBytes == block_size {

		// We are allocating comp_chunk extra here to know length
		// ! Fork snappy to return len(comp_buf.Buf) instead of
		// the the actual slice of comp_buf.Buf
		comp_chunk := snappy.Encode(comp_buf.Buf, block.Buf)
		//fmt.Println("Compressing block", block.BlockID, block_size, len(comp_chunk))

		// this misses the point of having reusable slices... :-(
		comp_buf.NBytes = len(comp_chunk)
		comp_buf.BlockID = block.BlockID

	} else {
		comp_buf.NBytes = block.NBytes
		comp_buf.BlockID = block.BlockID
		copy(comp_buf.Buf[:comp_buf.NBytes], block.Buf)
	}

	in.Upstream <- block
	out.Downstream <- comp_buf
}

func block_processor(in DuplexPipe, block_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}
	comp_len := snappy.MaxEncodedLen(block_size)
	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, comp_len), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go comp(<-out.Upstream, block, in, out, block_size, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return out
}

func block_writer(in DuplexPipe, filePath string) bool {

	// open descriptor file
	desc, err := os.Create(filePath + ".gob")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := desc.Close(); err != nil {
			panic(err)
		}
	}()

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

	offsets := make(map[int]Offset)

	i := 0
	for block := range in.Downstream {
		file.Write(block.Buf[:block.NBytes])
		offsets[block.BlockID] = Offset{i, block.NBytes}
		i += block.NBytes
		in.Upstream <- block
	}

	dataEncoder := gob.NewEncoder(desc)
	dataEncoder.Encode(offsets)

	return true
}

func main() {

	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])

	block_writer(
		block_processor(
			block_shuffler(
				block_generator(os.Args[1], block_size, 4),
				block_size, type_size, 4),
			block_size, 4),
		"output.bin")

}
