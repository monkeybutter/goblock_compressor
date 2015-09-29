package main

import (
	"encoding/binary"
	"./snappy"
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
		comp_block.Buf, comp_block.NBytes = snappy.Encode(comp_block.Buf, block.Buf)
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

func block_writer(in chan *Block, inFilePath, outFilePath string) bool {

	fi, err := os.Open(inFilePath)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	stat, err := fi.Stat()
	if err != nil {
	  // Could not obtain stat, handle error
	}

	nBlocks := (stat.Size()/(256*kB)) + 1

	// open output file
	file, err := os.Create(outFilePath)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	//Write First part of Header (vBlosc, vCompressor, flags, typeSize)
	file.Write([]byte{1, 1, 65, 64, 0, 0, 0, 0})

	//Write First part of Header (nbytes, blocksize, ctbytes)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(stat.Size()))
	file.Write(bs)
	binary.LittleEndian.PutUint32(bs, 256)
	file.Write(bs)
	binary.LittleEndian.PutUint32(bs, 0)
	file.Write(bs)

	//Reserve space for offsets
	offsets := make([]byte, 8*nBlocks)
	//Write offsets
	file.Write(offsets)

	var start uint32 = 20+uint32(8*nBlocks)

	for block := range in {
		file.Write(block.Buf[:block.NBytes])
		//Start Offset
		binary.LittleEndian.PutUint32(offsets[block.BlockID*8:(block.BlockID*8)+4], start)
		//Size of block
		binary.LittleEndian.PutUint32(offsets[(block.BlockID*8)+4:(block.BlockID*8)+8], uint32(block.NBytes))
		start += uint32(block.NBytes)
	}

	file.Seek(20, 0)
	file.Write(offsets)

	return true
}

func main() {
	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])

	block_writer(block_processor(block_shuffler(block_generator(os.Args[1], block_size, 4), block_size, type_size, 4), block_size, 4), os.Args[1], "output.bin")

}
