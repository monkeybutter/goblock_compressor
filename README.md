# goblock_compressor
Go block compression tests

Change Log:

* 17/07/2015: concompressor.go first approach to a concurrent block compressor using Go.

* 19/07/2015: concread.go program to test the effect of reading a file concurrently. To test times memory cache has to be deleted every time (use 'sudo purge' in macosx).
 
* 20/07/2015: concompressor.go reimplemented using concurrency only for compressing and writing (reading concurrently from a file makes no difference as its limited by the reading capacity of the disk head). After some tests there is no difference in the times used to read a file in chunks concurrently or serially. 

* 22/07/2015: concompressor.go snappy compression introduced using pure Go golang/snappy library. Results:

* 31/07/2015: conc_comp_pipeline alpha version of a concurrent pipeline model is introduced with high modularity and functional style execution. Concurrency level can be defined as parameters of the different pipe functions.

* 06/08/2015: conc_comp_pipeline has been modified to accept command line arguments to specify the number of processors, block size and input file.

* 12/08/2015: conc_comp_pipeline has been modified to include type size as a parameter.

* 20/08/2015: blocker_simple simplified version of the block compressor with a significant improvement in performance.

* 08/09/2015: blocker_simple_dumb most simple version of the block compressor which relies on the GC to clean allocations. Not as efficient as the previous version that makes use of the DuplexChannel but could be useful to understand the basics.

* 02/10/2015: unblocker_simple version of a concurrent block decompressor which returns the original file from the one compressed with the blocker_simple program. Benchmarks were run on the Raspberry Pi 2. Some polishing has to be made to the functions to comply with Go style and making them more simple.

* 09/10/2015: Changed structure of functions to blocker.go and filter.go. This gives it a package structure in the workspace. Own implementation of snappy has been moved out as a new separate package. Test suit has to be implemented to include input file creation, compression, decompression and cleaning.

Test A: compress the file

    np.save("npy1e8.bin", np.arange(1e8)) 763MB

        goroutines (threads) = 4

| Block  Size  kB | Time s  |
| --------------- | -------:|
| 1               | 8.20    |
| 8               | 2.95    |
| 32              | 2.43    |
| 128             | 2.18    |
| 256             | 1.98    |

No significant improvement achieved for chunks bigger than 256 kB

Test B: compress the file:

    np.save("input.bin", np.arange(1e8)) 763MB
 
        Block size = 256 kB

| Goroutines      | Time s  |
| --------------- | -------:|
| 1               | 4.47    |
| 2               | 2.66    |
| 3               | 2.24    |
| 4               | 2.04    |

Test B: shuffle and compress the file:

    np.save("input.bin", np.arange(1e8)) 763MB
 
        Block size = 256 kB

| Goroutines      | Time s  |
| --------------- | -------:|
| 1               | 6.36    |
| 2               | 3.41    |
| 3               | 2.50    |
| 4               | 2.26    |


Test C: Difference between compressing the files:

    a) np.save("input.bin", np.arange(1e8)) 763MB
    b) np.save("input.bin", np.arange(5e8)) 3.7GB

        Block size = 256 kB
        goroutines (threads) = 4

| File      | Time s  | Snappy Comp Rate %  |
| --------- | -------:| -------------------:|
| a         | 2.23    | 49.3                |
| b         | 8.68    | 51.4                |


| File      | Time s  | Shuffling + Snappy Comp Rate %  |
| --------- | -------:|--------------------------------:|
| a         | 2.35    | 28.8                            |
| b         | 9.84    | 32.4                            |
