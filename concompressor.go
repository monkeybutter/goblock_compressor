package main

import (
    "io"
    "bytes"
    "compress/gzip"
    "os"
)


// number of goroutines
const n_goroutines = 8

// channel for storing goroutines
var routines_chan = make(chan bool, n_goroutines)

func compress(chunk []byte, n int) []byte{
    var buf bytes.Buffer
    writer := gzip.NewWriter(&buf)
    writer.Write(chunk[:n])
    writer.Close()

    return buf.Bytes()
}

func write_chunk(fi io.Reader, fo io.Writer) {

    // define block size
    buf := make([]byte, 1024)

    for {
        // read a chunk
        n, err := fi.Read(buf)
        if err != nil && err != io.EOF {
            panic(err)
        }
        if n == 0 {
            break
        }

        // write a chunk
        if _, err := fo.Write(compress(buf, n)); err != nil {
            panic(err)
        }

    }

    routines_chan <- true

}

func main() {

    // open input file
    fi, err := os.Open("input.txt")
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
    fo, err := os.Create("output.txt")
    if err != nil {
        panic(err)
    }
    // close fo on exit and check for its returned error
    defer func() {
        if err := fo.Close(); err != nil {
            panic(err)
        }
    }()

    // Distribute work into routines
    for i := 0; i < n_goroutines; i++ {
        go write_chunk(fi, fo)
    }

    // Wait for them to finish
    for i := 0; i < n_goroutines; i++ {
        <- routines_chan
    }
    
}
