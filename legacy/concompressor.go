package main

import (
	"bytes"
	"compress/gzip"
	"github.com/golang/snappy"
	"io"
	"os"
	"runtime"
)

// number of goroutines
const n_goroutines = 4

// channel for storing goroutines
var routines_chan = make(chan bool, n_goroutines)

func gzip_compress(chunk []byte, n int) []byte {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	writer.Write(chunk[:n])
	writer.Close()

	return buf.Bytes()
}

func snappy_compress(chunk []byte, n int) []byte {
	var buf bytes.Buffer
	writer := snappy.NewWriter(&buf)
	writer.Write(chunk[:n])

	return buf.Bytes()
}

func write_chunk(buffer []byte, n int, fo io.Writer, compressor string) {

	// write a chunk
	if compressor == "snappy" {
		if _, err := fo.Write(snappy_compress(buffer, n)); err != nil {
			panic(err)
		}
	} else {
		if _, err := fo.Write(gzip_compress(buffer, n)); err != nil {
			panic(err)
		}
	}

	<-routines_chan

}

func main() {

	runtime.GOMAXPROCS(1)

	// open input file
	fi, err := os.Open("npy1e7.bin")
	if err != nil {
		panic(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	// open output file
	fo, err := os.Create("output.bin")
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	// define block size
	buf := make([]byte, 512*1024)

	for {
		// read a chunk
		n, err := fi.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		routines_chan <- true
		go write_chunk(buf, n, fo, "snappy")
	}

	// Wait for them to finish
	for i := 0; i < n_goroutines; i++ {
		routines_chan <- true
	}

}
