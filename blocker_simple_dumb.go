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

/*
type DuplexPipe struct {
	Downstream chan *Block
	Upstream   chan *Block
}*/

func block_generator(filePath string, block_size, conc_level int) chan *Block {

	// This is the output of the generator
	out := make(chan *Block, conc_level)

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

		for i := 0; ; i++ {
			buf := &Block{make([]byte, block_size), 0, 0}

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

			out <- buf

		}

		close(out)

	}()

	return out
}

func shuff(block *Block, out chan *Block, block_size, type_size int, wg *sync.WaitGroup) {

	defer wg.Done()
	shuff_block := &Block{make([]byte, block_size), 0, 0}

	if block.NBytes == block_size {

		shuff_unit := block_size / type_size

		for i := 0; i < type_size; i++ {
			shuff_offset := i * shuff_unit

			for j := 0; j < shuff_unit; j++ {
				shuff_block.Buf[shuff_offset+j] = block.Buf[j*type_size+i]
			}
		}

		shuff_block.NBytes = block.NBytes
		shuff_block.BlockID = block.BlockID

	} else {
		shuff_block.NBytes = block.NBytes
		shuff_block.BlockID = block.BlockID
		copy(shuff_block.Buf[:shuff_block.NBytes], block.Buf)
	}

	out <- shuff_block
}

func block_shuffler(in chan *Block, block_size, type_size, conc_level int) chan *Block {

	out := make(chan *Block, conc_level)

	var wg sync.WaitGroup

	go func() {

		for block := range in {
			wg.Add(1)
			go shuff(block, out, block_size, type_size, &wg)
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func comp(block *Block, out chan *Block, block_size int, wg *sync.WaitGroup) {

	defer wg.Done()

	comp_len := snappy.MaxEncodedLen(block_size)
	comp_block := &Block{make([]byte, comp_len), 0, 0}

	if block.NBytes == block_size {
		comp_block.Buf = snappy.Encode(comp_block.Buf, block.Buf)
		comp_block.NBytes = len(comp_block.Buf)
		comp_block.BlockID = block.BlockID

	} else {
		comp_block.NBytes = block.NBytes
		comp_block.BlockID = block.BlockID
		copy(comp_block.Buf[:comp_block.NBytes], block.Buf)
	}

	out <- comp_block
}

func block_processor(in chan *Block, block_size, conc_level int) chan *Block {

	out := make(chan *Block, conc_level)

	var wg sync.WaitGroup

	go func() {

		for block := range in {
			wg.Add(1)
			go comp(block, out, block_size, &wg)
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func block_writer(in chan *Block, filePath string) bool {

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
	for block := range in {
		file.Write(block.Buf[:block.NBytes])
		offsets[block.BlockID] = Offset{i, block.NBytes}
		i += block.NBytes
	}

	dataEncoder := gob.NewEncoder(desc)
	dataEncoder.Encode(offsets)

	return true
}

func main() {
	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])

	block_writer(block_processor(block_shuffler(block_generator(os.Args[1], block_size, 4), block_size, type_size, 4), block_size, 4), "output.bin")

}
