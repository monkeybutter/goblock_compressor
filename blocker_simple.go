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
		_, comp_buf.NBytes = snappy.Encode(comp_buf.Buf, block.Buf)
		//fmt.Println("Compressing block", block.BlockID, block_size, len(comp_chunk))

		// this misses the point of having reusable slices... :-(
		//comp_buf.NBytes = len(comp_chunk)
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

func block_writer(in DuplexPipe, inFilePath, outFilePath string) bool {

	// open input file
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
	fo, err := os.Create(outFilePath)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	//Write First part of Header (vBlosc, vCompressor, flags, typeSize)
	fo.Write([]byte{1, 1, 65, 64, 0, 0, 0, 0})

	//Write First part of Header (nbytes, blocksize, ctbytes)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(stat.Size()))
	fo.Write(bs)
	binary.LittleEndian.PutUint32(bs, 256)
	fo.Write(bs)
	binary.LittleEndian.PutUint32(bs, 0)
	fo.Write(bs)

	//Reserve space for offsets
	offsets := make([]byte, 8*nBlocks)
	//Write offsets
	fo.Write(offsets)

	var start uint32 = 20+uint32(8*nBlocks)

	for block := range in.Downstream {
		fo.Write(block.Buf[:block.NBytes])
		//Start Offset
		binary.LittleEndian.PutUint32(offsets[block.BlockID*8:(block.BlockID*8)+4], start)
		//Size of block
		binary.LittleEndian.PutUint32(offsets[(block.BlockID*8)+4:(block.BlockID*8)+8], uint32(block.NBytes))
		start += uint32(block.NBytes)

		in.Upstream <- block
	}

	fo.Seek(20, 0)
	fo.Write(offsets)

	return true
}

func main() {

	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB

	type_size, _ := strconv.Atoi(os.Args[3])

	conc_level, _ := strconv.Atoi(os.Args[4])

	block_writer(
		block_processor(
			block_shuffler(
				block_generator(os.Args[1], block_size, conc_level),
				block_size, type_size, conc_level),
			block_size, conc_level),
		os.Args[1], "output.bin")

}
