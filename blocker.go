package blockcompressor

import (
	"encoding/binary"
	"io"
	"os"
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
	FileSize   int64
	BlockSize  int
	TypeSize   int
	ConcLevel  int
}

type filter func(outBlock, inBlock *Block, inDuplexPipe, outDuplexPipe DuplexPipe, wg *sync.WaitGroup)

func blockGenerator(filePath string, blockSize, typeSize, concLevel int) (out DuplexPipe) {

	fi, err := os.Open(filePath)

	if err != nil {
		panic(err)
	}

	stat, err := fi.Stat()
	if err != nil {
		// Could not obtain stat, handle error
	}

	fileSize := stat.Size()

	if err := fi.Close(); err != nil {
		panic(err)
	}

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, concLevel), make(chan *Block, concLevel), fileSize, blockSize, typeSize, concLevel}

	// Block ready to hold reading
	for i := 0; i < concLevel; i++ {
		out.Upstream <- &Block{make([]byte, blockSize), 0, 0}
	}

	go func() {

		var buf *Block

		fi, err := os.Open(filePath)

		if err != nil {
			panic(err)
		}

		defer func() {
			if err := fi.Close(); err != nil {
				panic(err)
			}
		}()

		for i := 0; ; i++ {
			buf = <-out.Upstream

			// create a block
			n, err := fi.Read(buf.Buf)
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

	return
}

func blockProcessor(in DuplexPipe, proc filter) (out DuplexPipe) {

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, in.ConcLevel), make(chan *Block, in.ConcLevel), in.FileSize, in.BlockSize, in.TypeSize, in.ConcLevel}

	for i := 0; i < in.ConcLevel; i++ {
		out.Upstream <- &Block{make([]byte, in.BlockSize), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go proc(<-out.Upstream, block, in, out, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return
}


func blockWriter(in DuplexPipe, outFilePath string) bool {

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

	nBlocks := (in.FileSize / int64(in.BlockSize)) + 1

	//Write First part of Header (vBlosc, vCompressor, flags, typeSize)
	fo.Write([]byte{1, 1, 65, 64, 0, 0, 0, 0})

	//Write First part of Header (nbytes, blocksize, ctbytes)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(in.FileSize))
	fo.Write(bs)
	binary.LittleEndian.PutUint32(bs, uint32(in.BlockSize))
	fo.Write(bs)
	binary.LittleEndian.PutUint32(bs, 0)
	fo.Write(bs)

	//Reserve space for offsets
	offsets := make([]byte, 8*nBlocks)
	//Write offsets
	fo.Write(offsets)

	var start uint32 = 20 + uint32(8*nBlocks)

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

func unblockGenerator(filePath string, concLevel int) (out DuplexPipe) {

	fi, err := os.Open(filePath)

	if err != nil {
		panic(err)
	}

	f32s := make([]byte, 4)

	fi.Read(f32s)
	typeSize := int(f32s[3])

	fi.Seek(8, 0)
	fi.Read(f32s)
	fileSize := binary.LittleEndian.Uint32(f32s)

	//fi.Seek(12, 0)
	fi.Read(f32s)
	blockSize := binary.LittleEndian.Uint32(f32s)

	nBlocks := (fileSize / blockSize) + 1

	offsets := make([]byte, nBlocks*8)
	fi.Seek(20, 0)
	fi.Read(offsets)

	if err := fi.Close(); err != nil {
		panic(err)
	}

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, concLevel), make(chan *Block, concLevel), int64(fileSize), int(blockSize), typeSize, concLevel}
	// Block ready to hold reading
	for i := 0; i < concLevel; i++ {
		out.Upstream <- &Block{make([]byte, blockSize), 0, 0}
	}

	go func() {

		dataFile, err := os.Open(filePath)

		if err != nil {
			panic(err)
		}

		defer func() {
			if err := dataFile.Close(); err != nil {
				panic(err)
			}
		}()

		var buf *Block

		for i := uint32(0); i < nBlocks; i++ {
			buf = <-out.Upstream

			start := binary.LittleEndian.Uint32(offsets[i*8 : (i*8)+4])
			size := binary.LittleEndian.Uint32(offsets[i*8+4 : (i*8)+8])
			buf.NBytes = int(size)
			buf.BlockID = int(i)

			dataFile.Seek(int64(start), 0)
			dataFile.Read(buf.Buf[:size])

			out.Downstream <- buf
		}

		close(out.Downstream)

	}()

	return out
}

func unblockWriter(in DuplexPipe, outFilePath string) bool {

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

	for block := range in.Downstream {
		fo.Seek(int64(block.BlockID*in.BlockSize), 0)
		fo.Write(block.Buf[:block.NBytes])

		in.Upstream <- block
	}

	return true
}
