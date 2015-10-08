package blockcompressor

import (
	"github.com/monkeybutter/snappy"
	"encoding/binary"
	"fmt"
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
	FileSize   int64
	BlockSize  int
	TypeSize   int
	ConcLevel  int
}

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

func shuff(shuff_buf, block *Block, in, out DuplexPipe, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.NBytes == in.BlockSize {

		shuff_unit := in.BlockSize / in.TypeSize

		for i := 0; i < in.TypeSize; i++ {
			shuff_offset := i * shuff_unit

			for j := 0; j < shuff_unit; j++ {
				shuff_buf.Buf[shuff_offset+j] = block.Buf[j*in.TypeSize+i]
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

func blockShuffler(in DuplexPipe) (out DuplexPipe) {

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, in.ConcLevel), make(chan *Block, in.ConcLevel), in.FileSize, in.BlockSize, in.TypeSize, in.ConcLevel}

	for i := 0; i < in.ConcLevel; i++ {
		out.Upstream <- &Block{make([]byte, in.BlockSize), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go shuff(<-out.Upstream, block, in, out, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return
}

func comp(comp_buf, block *Block, in, out DuplexPipe, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.NBytes == in.BlockSize {

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

func blockProcessor(in DuplexPipe) (out DuplexPipe) {

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, in.ConcLevel), make(chan *Block, in.ConcLevel), in.FileSize, in.BlockSize, in.TypeSize, in.ConcLevel}

	comp_len := snappy.MaxEncodedLen(in.BlockSize)
	for i := 0; i < in.ConcLevel; i++ {
		out.Upstream <- &Block{make([]byte, comp_len), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go comp(<-out.Upstream, block, in, out, &wg)
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
	fmt.Println(nBlocks, in.BlockSize, in.FileSize)

	//Write First part of Header (vBlosc, vCompressor, flags, typeSize)
	fo.Write([]byte{1, 1, 65, 64, 0, 0, 0, 0})

	//Write First part of Header (nbytes, blocksize, ctbytes)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(in.FileSize))
	fo.Write(bs)
	binary.LittleEndian.PutUint32(bs, 256)
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

func main() {

	blockSize, _ := strconv.Atoi(os.Args[2])
	blockSize = blockSize * kB
	typeSize, _ := strconv.Atoi(os.Args[3])
	concLevel, _ := strconv.Atoi(os.Args[4])

	readerBlocks := blockGenerator(os.Args[1], blockSize, typeSize, concLevel)
	compressorBlocks := blockProcessor(blockShuffler(readerBlocks))
	blockWriter(compressorBlocks, "output.bin")
}
