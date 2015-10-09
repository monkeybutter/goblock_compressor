package blockcompressor

import (
	"github.com/monkeybutter/snappy"
	"encoding/binary"
	"strconv"
	"sync"
)

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

func decomp(decomp_buf, block *Block, in, out DuplexPipe, wg *sync.WaitGroup) {

	defer wg.Done()

	nBlocks := (in.FileSize / int64(in.BlockSize)) + 1

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

func unshuff(unshuff_buf, block *Block, in, out DuplexPipe, wg *sync.WaitGroup) {

	defer wg.Done()

	nBlocks := (in.FileSize / int64(in.BlockSize)) + 1

	if block.BlockID < int(nBlocks) - 1 {

		for j := 0; j < in.TypeSize; j++ {
			for k := 0; k < in.BlockSize/64; k++ {
				unshuff_buf.Buf[k*in.TypeSize+j] = block.Buf[k+(j*in.BlockSize/in.TypeSize)]
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