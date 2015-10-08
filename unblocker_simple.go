package blockcompressor

import (
	"github.com/monkeybutter/snappy"
	"os"
	"encoding/binary"
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

func block_reader(filePath string, block_size, conc_level int) (out DuplexPipe, nBlocks uint32) {

	// This is the output of the generator
	out = DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}
	// Block ready to hold reading
	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, block_size), 0, 0}
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

		f32s := make([]byte, 4)

		dataFile.Seek(8, 0)
		dataFile.Read(f32s)
		file_size := binary.LittleEndian.Uint32(f32s)

		dataFile.Seek(12, 0)
		dataFile.Read(f32s)
		block_size := binary.LittleEndian.Uint32(f32s) * kB

		nBlocks = (file_size / (block_size)) + 1

		offsets := make([]byte, nBlocks*8)
		dataFile.Seek(20, 0)
		dataFile.Read(offsets)

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

	return out, nBlocks
}

func decomp(decomp_buf, block *Block, in, out DuplexPipe, block_size int, nBlocks uint32, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.BlockID < int(nBlocks) - 1 {
		snappy.Decode(decomp_buf.Buf, block.Buf[:block.NBytes])
		decomp_buf.BlockID = block.BlockID

	} else {
		decomp_buf.NBytes = block.NBytes
		decomp_buf.BlockID = block.BlockID
		copy(decomp_buf.Buf[:decomp_buf.NBytes], block.Buf)
	}

	in.Upstream <- block
	out.Downstream <- decomp_buf
}

func block_processor(in DuplexPipe, nBlocks uint32, block_size, conc_level int) (DuplexPipe, uint32) {

	// This is the output of the generator
	out := DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}

	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, block_size), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go decomp(<-out.Upstream, block, in, out, block_size, nBlocks, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return out, nBlocks
}

func unshuff(unshuff_buf, block *Block, in, out DuplexPipe, block_size, type_size int, nBlocks uint32, wg *sync.WaitGroup) {

	defer wg.Done()

	if block.BlockID < int(nBlocks) - 1 {

		for j := 0; j < type_size; j++ {
			for k := 0; k < int(block_size)/64; k++ {
				unshuff_buf.Buf[k*type_size+j] = block.Buf[k+(j*int(block_size)/type_size)]
			}
		}

		unshuff_buf.NBytes = block.NBytes
		unshuff_buf.BlockID = block.BlockID

	} else {
		unshuff_buf.NBytes = block.NBytes
		unshuff_buf.BlockID = block.BlockID
		copy(unshuff_buf.Buf[:unshuff_buf.NBytes], block.Buf)
	}

	in.Upstream <- block
	out.Downstream <- unshuff_buf
}

func block_unshuffler(in DuplexPipe, nBlocks uint32, block_size, type_size, conc_level int) DuplexPipe {

	// This is the output of the generator
	out := DuplexPipe{make(chan *Block, conc_level), make(chan *Block, conc_level)}

	for i := 0; i < conc_level; i++ {
		out.Upstream <- &Block{make([]byte, block_size), 0, 0}
	}

	var wg sync.WaitGroup

	go func() {

		for block := range in.Downstream {
			wg.Add(1)
			go unshuff(<-out.Upstream, block, in, out, block_size, type_size, nBlocks, &wg)
		}

		wg.Wait()
		close(out.Downstream)
	}()

	return out
}

func block_writer(in DuplexPipe, block_size int, outFilePath string) bool {

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
		fo.Seek(int64(block.BlockID*block_size), 0)
		fo.Write(block.Buf[:block.NBytes])

		in.Upstream <- block
	}

	return true
}

func main() {

	block_size, _ := strconv.Atoi(os.Args[2])
	block_size = block_size * kB
	type_size, _ := strconv.Atoi(os.Args[3])
	conc_level, _ := strconv.Atoi(os.Args[4])

	out1, nBlocks := block_reader(os.Args[1], block_size, conc_level)
  out2, _ := block_processor(out1, nBlocks, block_size, conc_level)
	out3 := block_unshuffler(out2, nBlocks, block_size, type_size, conc_level)
	block_writer(out3, block_size, "output.bin.ini")

}
