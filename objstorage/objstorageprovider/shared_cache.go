// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

type sharedCache struct {
	shards []sharedCacheShard
	logger base.Logger

	// TODO(josh): Have a dedicated metrics struct. Right now, this
	// is just for testing.
	misses atomic.Int32
}

func openSharedCache(
	fs vfs.FS, fsDir string, blockSize int, sizeBytes int64, numShards int,
) (*sharedCache, error) {
	min := shardingBlockSize * int64(numShards)
	if sizeBytes < min {
		return nil, errors.Errorf("cache size %d lower than min %d", sizeBytes, min)
	}

	sc := &sharedCache{}
	sc.shards = make([]sharedCacheShard, numShards)
	blocksPerShard := sizeBytes / int64(numShards) / int64(blockSize)
	for i := range sc.shards {
		if err := sc.shards[i].init(fs, fsDir, i, blocksPerShard, blockSize); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

func (sc *sharedCache) Close() error {
	var retErr error
	for i := range sc.shards {
		if err := sc.shards[i].Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	sc.shards = nil
	return retErr
}

// ReadAt performs a read form an object, attempting to use cached data when
// possible.
func (sc *sharedCache) ReadAt(
	ctx context.Context, fileNum base.FileNum, p []byte, ofs int64, readable objstorage.Readable,
) error {
	n, err := sc.Get(fileNum, p, ofs)
	if err != nil {
		return err
	}
	if n == len(p) {
		// Everything was in cache!
		return nil
	}
	// Note this. The below code does not need the original ofs, as with the earlier
	// reading from the cache done, the relevant offset is ofs + int64(n).
	ofs += int64(n)

	// We must do reads with offset & size that are multiples of the block size. Else
	// later cache hits may return incorrect zeroed results from the cache. We assume
	// that all shards have the same block size.
	blockSize := sc.shards[0].blockSize

	firstBlockInd := ofs / int64(blockSize)
	adjustedOfs := firstBlockInd * int64(blockSize)

	// Take the length of what is left to read plus the length of the adjustment of
	// the offset plus the size of a block minus one and divide by the size of a block
	// to get the number of blocks to read from the readable.
	numBlocksToRead := ((len(p[n:]) + int(ofs-adjustedOfs)) + (blockSize - 1)) / blockSize
	adjustedLen := numBlocksToRead * blockSize
	adjustedP := make([]byte, adjustedLen)

	// Read the rest from the object.
	sc.misses.Add(1)
	// TODO(josh): To have proper EOF handling, we will need readable.ReadAt to return
	// the number of bytes read successfully. As is, we cannot tell if the readable.ReadAt
	// should be returned from sharedCache.ReadAt. For now, the cache just swallows all
	// io.EOF errors.
	if err := readable.ReadAt(ctx, adjustedP, adjustedOfs); err != nil && err != io.EOF {
		return err
	}
	copy(p[n:], adjustedP[ofs%int64(blockSize):])

	// TODO(josh): Writing back to the cache should be async with respect to the
	// call to ReadAt.
	if err := sc.Set(fileNum, adjustedP, adjustedOfs); err != nil {
		// TODO(josh): Would like to log at error severity, but base.Logger doesn't
		// have error severity.
		sc.logger.Infof("writing back to cache after miss failed: %v", err)
	}
	return nil
}

// Get attempts to read the requested data from the cache.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (sc *sharedCache) Get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		shard := sc.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - ((ofs + int64(n)) % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		numRead, err := shard.Get(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return n, err
		}
		n += numRead
		if numRead < cappedLen {
			// We only read a prefix from this shard.
			return n, nil
		}
		if n == len(p) {
			// We are done.
			return n, nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

// Set attempts to write the requested data to the cache. Both ofs & len(p) must
// be multiples of the block size.
//
// If all of p is not written to the shard, Set returns a non-nil error.
func (sc *sharedCache) Set(fileNum base.FileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs%int64(sc.shards[0].blockSize) != 0 || len(p)%sc.shards[0].blockSize != 0 {
			panic(fmt.Sprintf("Set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
		}
	}

	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		shard := sc.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - ((ofs + int64(n)) % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		err := shard.Set(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return err
		}
		// Set returns an error if cappedLen bytes aren't written the the shard.
		n += cappedLen
		if n == len(p) {
			// We are done.
			return nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

const shardingBlockSize = 1024 * 1024

func (sc *sharedCache) getShard(fileNum base.FileNum, ofs int64) *sharedCacheShard {
	const prime64 = 1099511628211
	hash := uint64(fileNum)*prime64 + uint64(ofs)/shardingBlockSize
	// TODO(josh): Instance change ops are often run in production. Such an operation
	// updates len(sc.shards); see openSharedCache. As a result, the behavior of this
	// function changes, and the cache empties out at restart time. We may want a better
	// story here eventually.
	return &sc.shards[hash%uint64(len(sc.shards))]
}

type sharedCacheShard struct {
	file         vfs.File
	numDataBlocks int64
	numMetadataBlocks int64
	blockSize    int
	mu           struct {
		sync.Mutex

		// TODO(josh): None of these datastructures are space-efficient.
		// Focusing on correctness to start.
		where map[metadataMapKey]int64
		table []*metadataMapKey
		free  []int64
		seq   int32
	}
	log []metadataLogEntryBatch
}

type metadataMapKey struct {
	filenum    base.FileNum
	logicalBlockInd int64
}

type metadataLogEntry struct {
	filenum    base.FileNum
	logicalBlockInd int64
	cacheBlockInd int64
}

// TODO(josh): We should make this type more space-efficient later on.
type metadataLogEntryBatch struct {
	e1 metadataLogEntry
	e2 metadataLogEntry
	seq int32
}

func (s *sharedCacheShard) init(
	fs vfs.FS, fsDir string, shardIdx int, sizeInBlocks int64, blockSize int,
) error {
	*s = sharedCacheShard{}
	if blockSize < 1024 || shardingBlockSize%blockSize != 0 {
		return errors.Newf("invalid block size %d (must divide %d)", blockSize, shardingBlockSize)
	}
	s.blockSize = blockSize
	file, err := fs.OpenReadWrite(fs.PathJoin(fsDir, fmt.Sprintf("SHARED-CACHE-%03d", shardIdx)))
	if err != nil {
		return err
	}
	// TODO(radu): truncate file if necessary (especially important if we restart
	// with more shards).
	if err := file.Preallocate(0, int64(blockSize)*sizeInBlocks); err != nil {
		return err
	}
	s.file = file

	// TODO(josh): For now, changing parameters of the cache, such as sizeInBlocks, is not
	// supported, if metadata persistence is enabled.
	metadataBatchSizeInBytes := int64(unsafe.Sizeof(metadataLogEntryBatch{}))
	// TODO(): Used to be 2x.
	metadataTotalSizeInBytes := metadataBatchSizeInBytes * sizeInBlocks
	numMetadataBlocks := (metadataTotalSizeInBytes + int64(blockSize)-1) / int64(blockSize)

	s.numMetadataBlocks = numMetadataBlocks
	s.numDataBlocks = sizeInBlocks - numMetadataBlocks

	logAsBytes := make([]byte, metadataTotalSizeInBytes)
	_, err = s.file.ReadAt(logAsBytes, 0)
	if err != nil && err != io.EOF {
		return err
	}

	// TODO(): Used to be 2x.
	logLen := s.numDataBlocks
	logPtr := unsafe.Pointer(&logAsBytes[0])
	log := unsafe.Slice((*metadataLogEntryBatch)(logPtr), logLen)
	s.log = log

	maxSeq := int32(0)
	maxEntry := 0
	for i, e := range log {
		if e.seq > maxSeq {
			maxSeq = e.seq
			maxEntry = i
		}
	}
	s.mu.seq = maxSeq + 1

	s.mu.table = make([]*metadataMapKey, s.numDataBlocks)
	// TODO(): Assert that cache block indexes increase as expected.
	var prevSeq int32
	for i := 0; int64(i) < logLen; i++ {
		entry := log[(i + maxEntry + 1) % int(logLen)]
		if entry.seq == 0 {
			continue
		}
		if i != 0 && entry.seq != prevSeq + 1 {
			return fmt.Errorf("sequence numbers not monotonically increasing: %v %v", prevSeq, entry.seq)
		}
		prevSeq = entry.seq
		for _, subEntry := range []metadataLogEntry{entry.e1, entry.e2} {
			// TODO(): Allow filenum = 0.
			if subEntry.filenum != 0 {
				k := metadataMapKey{
					filenum:         subEntry.filenum,
					logicalBlockInd: subEntry.logicalBlockInd,
				}
				s.mu.table[subEntry.cacheBlockInd] = &k
			}
		}
	}

	s.mu.where = make(map[metadataMapKey]int64)
	for cacheBlockInd, e := range s.mu.table {
		if e != nil {
			s.mu.where[*e] = int64(cacheBlockInd)
		} else {
			s.mu.free = append(s.mu.free, int64(cacheBlockInd))
		}
	}

	return nil
}

func (s *sharedCacheShard) Close() error {
	defer func() {
		s.file = nil
	}()
	return s.file.Close()
}

// Get attempts to read the requested data from the shard. The data must not
// cross a shard boundary.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (s *sharedCacheShard) Get(fileNum base.FileNum, p []byte, ofs int64) (n int, _ error) {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic(fmt.Sprintf("Get crosses shard boundary: %v %v", ofs, len(p)))
		}
	}

	// TODO(josh): Make the locking more fine-grained. Do not hold locks during calls
	// to ReadAt.
	s.mu.Lock()
	defer s.mu.Unlock()

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		cacheBlockInd, ok := s.mu.where[metadataMapKey{
			filenum:    fileNum,
			logicalBlockInd: (ofs + int64(n)) / int64(s.blockSize),
		}]
		if !ok {
			return n, nil
		}

		readAt := cacheBlockInd * int64(s.blockSize) + s.numMetadataBlocks * int64(s.blockSize)
		if n == 0 { // if first read
			readAt += ofs % int64(s.blockSize)
		}

		readSize := s.blockSize
		if n == 0 { // if first read
			// Cast to int safe since ofs is modded by block size.
			readSize -= int(ofs % int64(s.blockSize))
		}
		if len(p[n:]) <= readSize {
			readSize = len(p[n:])
		}

		if readAt + int64(readSize) < s.numMetadataBlocks * int64(s.blockSize) {
			panic(fmt.Sprintf("reading a data block from the metadata block section: %v %v %v %v", readAt, readSize, s.numMetadataBlocks, s.blockSize))
		}

		if len(p[n:]) <= readSize {
			numRead, err := s.file.ReadAt(p[n:n+readSize], readAt)
			return n + numRead, err
		}
		numRead, err := s.file.ReadAt(p[n:n+readSize], readAt)
		if err != nil {
			return 0, err
		}

		// Note that numRead == readSize, since we checked for an error above.
		n += numRead
	}
}

// Set attempts to write the requested data to the shard. The data must not
// cross a shard boundary, and both ofs & len(p) must be multiples of the
// block size.
//
// If all of p is not written to the shard, Set returns a non-nil error.
func (s *sharedCacheShard) Set(fileNum base.FileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic(fmt.Sprintf("Set crosses shard boundary: %v %v", ofs, len(p)))
		}
		if ofs%int64(s.blockSize) != 0 || len(p)%s.blockSize != 0 {
			panic(fmt.Sprintf("Set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
		}
	}

	// TODO(josh): Make the locking more fine-grained. Do not hold locks during calls
	// to WriteAt.
	s.mu.Lock()
	defer s.mu.Unlock()

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		var cacheBlockInd int64
		if len(s.mu.free) == 0 {
			// TODO(josh): Right now, we do random eviction. Eventually, we will do something
			// more sophisticated, e.g. leverage ClockPro.
			var k metadataMapKey
			for k1, v := range s.mu.where {
				cacheBlockInd = v
				k = k1
				break
			}
			delete(s.mu.where, k)
		} else {
			cacheBlockInd = s.mu.free[len(s.mu.free)-1]
			s.mu.free = s.mu.free[:len(s.mu.free)-1]
		}

		k := metadataMapKey{
			filenum:    fileNum,
		logicalBlockInd: (ofs + int64(n)) / int64(s.blockSize),
		}
		s.mu.where[k] = cacheBlockInd
		s.mu.table[cacheBlockInd] = &k

		writeAt := cacheBlockInd * int64(s.blockSize) + s.numMetadataBlocks * int64(s.blockSize)
		writeSize := s.blockSize
		if writeAt + int64(writeSize) < s.numMetadataBlocks * int64(s.blockSize) {
			panic(fmt.Sprintf("writing a data block to the metadata block section: %v %v %v %v", writeAt, writeSize, s.numMetadataBlocks, s.blockSize))
		}

		entry := metadataLogEntry{
			filenum: k.filenum,
			logicalBlockInd: k.logicalBlockInd,
			cacheBlockInd: cacheBlockInd,
		}

		if len(p[n:]) <= writeSize {
			// Ignore num written ret value, since if partial write, an error
			// is returned.
			_, err := s.file.WriteAt(p[n:], writeAt)
			if err != nil {
				return err
			}

			// TODO(josh): It is not safe to update the contents of the cache & the metadata in two
			// disk writes like this, without the working set scheme described by Radu at
			// https://github.com/cockroachdb/pebble/issues/2542. We will implement that later.
			return s.persistMetadataUpdate(entry)
		}

		numWritten, err := s.file.WriteAt(p[n:n+writeSize], writeAt)
		if err != nil {
			return err
		}

		if err := s.persistMetadataUpdate(entry); err != nil {
			return err
		}

		// Note that numWritten == writeSize, since we checked for an error above.
		n += numWritten
	}
}

func (s *sharedCacheShard) persistMetadataUpdate(e metadataLogEntry) error {
	ptr := int64(s.mu.seq) % s.numDataBlocks
	batch := metadataLogEntryBatch{
		e1:  e,
		seq: s.mu.seq,
	}
	if s.mu.table[ptr] != nil {
		batch.e2 = metadataLogEntry{
			filenum: s.mu.table[ptr].filenum,
			logicalBlockInd: s.mu.table[ptr].logicalBlockInd,
			cacheBlockInd: ptr,
		}
	}

	// TODO(josh): This serialization scheme is not intended to be portable across machine
	// architectures. We can make it portable in a later PR.
	batchAsBytes := (*[unsafe.Sizeof(metadataLogEntryBatch{})]byte)(unsafe.Pointer(&batch))[:]
	// TODO(): Used to be 2x.
	writeAt := int64(s.mu.seq) % s.numDataBlocks * int64(unsafe.Sizeof(metadataLogEntryBatch{}))
	if writeAt + int64(len(batchAsBytes)) > s.numMetadataBlocks * int64(s.blockSize) {
		panic(fmt.Sprintf("writing a metadata block to the data block section: %v %v %v %v", writeAt, len(batchAsBytes), s.numMetadataBlocks, s.blockSize))
	}

	_, err := s.file.WriteAt(batchAsBytes, writeAt)
	if err != nil {
		return err
	}

	// TODO(): Keep seq from overflowing, by moding by something, or similar.
	s.mu.seq++ // mu is held; see Set; fine-grained locking can be done later on
	return nil
}
