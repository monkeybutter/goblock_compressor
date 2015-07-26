package main

import (
    "encoding/binary"
    "fmt"
    "math"
)

var shuff_chan = make(chan bool, 8)
var unshuff_chan = make(chan bool, 8)


func Float64frombytes(bytes []byte) float64 {
    bits := binary.LittleEndian.Uint64(bytes)
    float := math.Float64frombits(bits)
    return float
}

func Float64_bytes(float float64) []byte {
    bits := math.Float64bits(float)
    bytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(bytes, bits)
    return bytes
}

func Float64slice_bytes(floatslice []float64) []byte {
    bytes := make([]byte, 8*len(floatslice))
    
    for i:=0; i<len(floatslice); i++ {
        bits := math.Float64bits(floatslice[i])
        binary.LittleEndian.PutUint64(bytes[8*i:8*(i+1)], bits)
        
    }
    
    return bytes
}

func shuffle_part_n(chunk, part []byte, n, size int) {

        for i:=0; i<len(part); i++ {
              part[i] = chunk[i*size+n]
        }
	shuff_chan <- true
}

func shuffle_chunk(chunk []byte, size int) []byte {

        shuffled_chunk := make([]byte, len(chunk))
	set := len(chunk)/size

        for i:=0; i<size; i++ {
              part := shuffled_chunk[set*i:set*(i+1)]
              go shuffle_part_n(chunk, part, i, size)
        }

	// Wait for them to finish
        for i := 0; i < size; i++ {
              <-shuff_chan
        }

	return shuffled_chunk
}

func unshuffle_part_n(chunk, shuffled_part []byte, n, size int) {

        for i:=0; i<len(shuffled_part); i++ {
              chunk[i*size+n] = shuffled_part[i]
        }
	unshuff_chan <- true
}

func unshuffle_chunk(chunk []byte, size int) []byte {

        unshuffled_chunk := make([]byte, len(chunk))
	set := len(chunk)/size

        for i:=0; i<size; i++ {
              shuffled_part := chunk[set*i:set*(i+1)]
              go unshuffle_part_n(unshuffled_chunk, shuffled_part, i, size)
        }
	
        // Wait for them to finish
        for i := 0; i < size; i++ {
              <-unshuff_chan
        }

	return unshuffled_chunk
}


func main() {
    bytes := Float64_bytes(math.Pi)
    fmt.Println(bytes)
    float := Float64frombytes(bytes)
    fmt.Println(float)
    bytes_slc := Float64slice_bytes([]float64{math.Pi, math.Pi, math.Pi, math.Pi})
    fmt.Println(bytes_slc)
    shuff := shuffle_chunk(bytes_slc, 8)
    fmt.Println(shuff)
    unshuff := unshuffle_chunk(shuff, 8)
    fmt.Println(unshuff)
}
