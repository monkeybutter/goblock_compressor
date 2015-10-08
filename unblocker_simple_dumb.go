package blockcompressor

import (
	"os"
	"encoding/binary"
	"github.com/golang/snappy"
)

const kB = 1024

type Block struct {
	Buf     []byte
	NBytes  int
	BlockID int
}

func block_reader(filePath string, conc_level int) chan *Block {

	// This is the output of the generator
	out := make(chan *Block, conc_level)

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
		bytes := make([]byte, 1)

		dataFile.Seek(3, 0)
		dataFile.Read(bytes)
		type_size := int(bytes[0])

		dataFile.Seek(8, 0)
		dataFile.Read(f32s)
		file_size := binary.LittleEndian.Uint32(f32s)

		dataFile.Seek(12, 0)
		dataFile.Read(f32s)
		block_size := binary.LittleEndian.Uint32(f32s) * kB

		nBlocks := (file_size/(block_size)) + 1

		offsets := make([]byte, nBlocks*8)
		dataFile.Seek(20, 0)
		dataFile.Read(offsets)


		for i := uint32(0); i<nBlocks; i++ {

			start := binary.LittleEndian.Uint32(offsets[i*8:(i*8)+4])
			size := binary.LittleEndian.Uint32(offsets[i*8+4:(i*8)+8])
			buf := &Block{make([]byte, size), size, 0}

			dataFile.Seek(int64(start), 0)
			dataFile.Read(buf.Buf)
			out <- buf
		}

		close(out)

	}()

	return out
}


func block_decomp(in DuplexPipe, block_size, conc_level int) DuplexPipe {

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



func block_unshuffler(in chan *Block, block_size, type_size, conc_level int) chan *Block {

	out := make(chan *Block, conc_level)

	var wg sync.WaitGroup

	go func() {

		for block := range in {
			wg.Add(1)
			go unshuff(block, out, block_size, type_size, &wg)
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func main() {
	// open input file
	dataFile, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := dataFile.Close(); err != nil {
			panic(err)
		}
	}()

	// open uncompressed file
	outFile, err := os.Create(os.Args[1] + ".ini")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := outFile.Close(); err != nil {
			panic(err)
		}
	}()

	f32s := make([]byte, 4)
	bytes := make([]byte, 1)

	dataFile.Seek(3, 0)
	dataFile.Read(bytes)
	type_size := int(bytes[0])

	dataFile.Seek(8, 0)
	dataFile.Read(f32s)
	file_size := binary.LittleEndian.Uint32(f32s)

	dataFile.Seek(12, 0)
	dataFile.Read(f32s)
	block_size := binary.LittleEndian.Uint32(f32s) * kB

	nBlocks := (file_size/(block_size)) + 1

	offsets := make([]byte, nBlocks*8)
	dataFile.Seek(20, 0)
	dataFile.Read(offsets)

	dbuf := make([]byte, block_size)

	for i := uint32(0); i<nBlocks-1; i++ {
		start := binary.LittleEndian.Uint32(offsets[i*8:(i*8)+4])
		size := binary.LittleEndian.Uint32(offsets[i*8+4:(i*8)+8])

		block := make([]byte, size)
		dataFile.Seek(int64(start), 0)
		dataFile.Read(block)
		d, _ := snappy.Decode(dbuf, block[:])

		bufo := make([]byte, len(d))

		for j := 0; j < type_size; j++ {
			for k := 0; k < int(block_size)/64; k++ {
				bufo[k*type_size+j] = d[k+(j*int(block_size)/type_size)]
			}
		}

		outFile.Write(bufo)
	}

	start := binary.LittleEndian.Uint32(offsets[(nBlocks-1)*8:((nBlocks-1)*8)+4])
	size := binary.LittleEndian.Uint32(offsets[(nBlocks-1)*8+4:((nBlocks-1)*8)+8])

	block := make([]byte, size)
	dataFile.Seek(int64(start), 0)
	dataFile.Read(block)
	outFile.Write(block)
}
