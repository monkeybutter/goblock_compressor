package main

import (
	"io"
	"os"
	"runtime"
)

// number of goroutines
const n_goroutines = 1

// kilobyte
const kb = 1024

// channel for storing goroutines
var routines_chan = make(chan bool, n_goroutines)

func read_chunk(fi io.Reader) {

	// define block size
	buf := make([]byte, 2*kb)

	for {
		// read a chunk
		n, err := fi.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

	}

	routines_chan <- true

}

func main() {
	runtime.GOMAXPROCS(1)
	// open input file
	fi, err := os.Open("input.bin")
	if err != nil {
		panic(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	// Distribute work into routines
	for i := 0; i < n_goroutines; i++ {
		go read_chunk(fi)
	}

	// Wait for them to finish
	for i := 0; i < n_goroutines; i++ {
		<-routines_chan
	}

}
