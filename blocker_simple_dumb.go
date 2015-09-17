package main

import (
	"encoding/gob"
	"encoding/binary"
	"fmt"
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
	Start uint32
	Size  uint32
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
	fmt.Println(nBlocks)

	// open descriptor file
	desc, err := os.Create(outFilePath + ".gob")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := desc.Close(); err != nil {
			panic(err)
		}
	}()

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
    binary.LittleEndian.PutUint32(bs, 0)
	file.Write(bs)
	binary.LittleEndian.PutUint32(bs, 256)
	file.Write(bs)
	binary.LittleEndian.PutUint32(bs, 0)
	file.Write(bs)

	//Reserve space for offsets
	offsets2 := make([]byte, 4*nBlocks)
	file.Write(offsets2)

	offsets := make(map[int]Offset)

	var start uint32 = 0
	i := 0
	for block := range in {
		file.Write(block.Buf[:block.NBytes])
		offsets[block.BlockID] = Offset{start, uint32(block.NBytes)}
		binary.LittleEndian.PutUint32(offsets2[i:i+4], start)
		//fmt.Println(start, offsets2[i:i+4], binary.BigEndian.Uint32(offsets2[i:i+4]))
		start += uint32(block.NBytes)
		i += 4
	}

	file.Seek(20, 0)
	file.Write(offsets2)

	file.Seek(0, 0)
	number := make([]byte, 4)
	for i:=0; i<20; i++ {
		file.Read(number)
		fmt.Println(binary.LittleEndian.Uint32(number))
	}

	dataEncoder := gob.NewEncoder(desc)
	dataEncoder.Encode(offsets)

	return true
}

func main() {
	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])

	block_writer(block_processor(block_shuffler(block_generator(os.Args[1], block_size, 4), block_size, type_size, 4), block_size, 4), os.Args[1], "output.bin")

}
