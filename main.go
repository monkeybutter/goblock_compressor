package main


func main() {

	blockSize, _ := strconv.Atoi(os.Args[2])
	blockSize = blockSize * kB
	typeSize, _ := strconv.Atoi(os.Args[3])
	concLevel, _ := strconv.Atoi(os.Args[4])

	readerBlocks := blockGenerator(os.Args[1], blockSize, typeSize, concLevel)
	compressorBlocks := blockProcessor(blockProcessor(readerBlocks, shuff), comp)
	blockWriter(compressorBlocks, "output.bin")
}


