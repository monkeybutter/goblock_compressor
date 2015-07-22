# goblock_compressor
Go block compression tests

Change Log:

17/07/2015: concompressor.go first approach to a concurrent concurrent block compressor using Go.

19/07/2015: concread.go program to test the effect of reading a file concurrently. To test times memory cache has to be deleted every time (use 'sudo purge' in macosx).
 
20/07/2015: concompressor.go reimplemented using concurrency only for compressing and writing (reading concurrently from a file makes no difference as its limited by the reading capacity of the disk head). After some tests there is no difference in the times used to read a file in chunks concurrently or serially. 

22/07/2015: concompressor.go snappy compression introduced using pure Go golang/snappy library. Results:

Test A: compress np.save("input.bin", np.arange(1e8)) 376MB

        goroutines (threads) = 4

| Buffer Size  kB | Time s  |
| --------------- | -------:|
| 1               | 21.49   |
| 8               | 5.45    |
| 32              | 2.94    |
| 64              | 2.94    |
| 128             | 2.13    |
| 256             | 2.05    |

No significant improvement achieved for chunks bigger than 256 kB

Test B: compress np.save("input.bin", np.arange(1e8)) 376MB
         
        Buffer size = 256 kB

| Goroutines      | Time s  |
| --------------- | -------:|
| 1               | 4.48    |
| 2               | 2.66    |
| 3               | 2.24    |
| 4               | 2.04    |

Test C: Difference between compressing the files:

   a) np.save("input.bin", np.arange(1e8)) 376MB
   b) np.save("input.bin", np.arange(3e8)) 1.1GB

        Buffer size = 256 kB
        goroutines (threads) = 4

| File      | Time s  | Compression Rate %  |
| --------- | -------:| -------------------:|
| a         | 2.05    | 49.3                |
| b         | 5.15    | 49.7                |
