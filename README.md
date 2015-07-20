# goblock_compressor
Go block compression tests

Change Log:

17/07/2015: concompressor.go first approach to a concurrent concurrent block compressor using Go.

19/07/2015: concreader.go program to test the effect of reading a file concurrently. To test times memory cache has to be deleted every time (use 'sudo purge' in macosx).
 
20/07/2015: concompressor.go reimplemented using concurrency only for compressing and writing (reading concurrently from a file makes no difference as its limited by the reading capacity of the disk head). After some tests there is no difference in the times used to read a file in chunks concurrently or serially. 
