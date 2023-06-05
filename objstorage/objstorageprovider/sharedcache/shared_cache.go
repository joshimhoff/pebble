// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedcache

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage/shared"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	shardingBlockSize = 1024 * 1024
)

// Cache is a persistent cache backed by a local filesystem. It is intended
// to cache data that is in slower shared storage (e.g. S3), hence the
// package name 'sharedcache'.
type Cache struct {
	shards []shard
	logger base.Logger

	blockSize int

	writeBackWaitGroup sync.WaitGroup
	// TODO(josh): Have a dedicated metrics struct. Right now, this
	// is just for testing.
	misses atomic.Int32
}

// Open opens a cache. If there is no existing cache at fsDir, a new one
// is created.
func Open(
	fs vfs.FS, logger base.Logger, fsDir string, blockSize int, sizeBytes int64, numShards int,
) (*Cache, error) {
	min := shardingBlockSize * int64(numShards)
	if sizeBytes < min {
		return nil, errors.Errorf("cache size %d lower than min %d", sizeBytes, min)
	}

	sc := &Cache{
		logger:    logger,
		blockSize: blockSize,
	}
	sc.shards = make([]shard, numShards)
	blocksPerShard := sizeBytes / int64(numShards) / int64(blockSize)
	for i := range sc.shards {
		if err := sc.shards[i].init(fs, fsDir, i, blocksPerShard, blockSize); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

// Close closes the cache. Methods such as ReadAt should not be called after Close is
// called.
func (c *Cache) Close() error {
	c.writeBackWaitGroup.Wait()

	var retErr error
	for i := range c.shards {
		if err := c.shards[i].close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	c.shards = nil
	return retErr
}

// ReadFlags contains options for Cache.ReadAt.
type ReadFlags struct {
	// ReadOnly instructs ReadAt to not write any new data into the cache; it is
	// used when the data is unlikely to be used again.
	ReadOnly bool
}

// ReadAt performs a read form an object, attempting to use cached data when
// possible.
func (c *Cache) ReadAt(
	ctx context.Context,
	fileNum base.DiskFileNum,
	p []byte,
	ofs int64,
	objReader shared.ObjectReader,
	flags ReadFlags,
) error {
	// TODO(radu): for compaction reads, we may not want to read from the cache at
	// all.
	{
		n, err := c.get(fileNum, p, ofs)
		if err != nil {
			return err
		}
		if n == len(p) {
			// Everything was in cache!
			return nil
		}

		// Note this. The below code does not need the original ofs, as with the earlier
		// reading from the cache done, the relevant offset is ofs + int64(n). Same with p.
		ofs += int64(n)
		p = p[n:]

		if invariants.Enabled {
			if n != 0 && ofs%int64(c.blockSize) != 0 {
				panic(fmt.Sprintf("after non-zero read from cache, ofs is not block-aligned: %v %v", ofs, n))
			}
		}
	}

	c.misses.Add(1)

	if flags.ReadOnly {
		return objReader.ReadAt(ctx, p, ofs)
	}

	// We must do reads with offset & size that are multiples of the block size. Else
	// later cache hits may return incorrect zeroed results from the cache.
	firstBlockInd := ofs / int64(c.blockSize)
	adjustedOfs := firstBlockInd * int64(c.blockSize)

	// Take the length of what is left to read plus the length of the adjustment of
	// the offset plus the size of a block minus one and divide by the size of a block
	// to get the number of blocks to read from the object.
	sizeOfOffAdjustment := int(ofs - adjustedOfs)
	numBlocksToRead := ((len(p) + sizeOfOffAdjustment) + (c.blockSize - 1)) / c.blockSize
	adjustedLen := numBlocksToRead * c.blockSize
	adjustedP := make([]byte, adjustedLen)

	// Read the rest from the object.
	// TODO(josh): To have proper EOF handling, we will need to use the Size method of the
	// readable to limit the size of a read when at the end of a file. For now, the cache
	// just swallows all io.EOF errors.
	if err := objReader.ReadAt(ctx, adjustedP, adjustedOfs); err != nil && err != io.EOF {
		return err
	}
	copy(p, adjustedP[sizeOfOffAdjustment:])

	// TODO(josh): For writing back to the cache, we may want a concurrency-limited approach
	// that does batching, instead of what is below. I think it's reasonable though to
	// run a production experiment with what is below, before making adjustments. Note that
	// the writes being done are in multiples of the filesystem block size. The filesystem
	// might do okay with them.
	c.writeBackWaitGroup.Add(1)
	go func() {
		defer c.writeBackWaitGroup.Done()
		if err := c.set(fileNum, adjustedP, adjustedOfs); err != nil {
			// TODO(josh): Would like to log at error severity, but base.Logger doesn't
			// have error severity.
			c.logger.Infof("writing back to cache after miss failed: %v", err)
		}
	}()
	return nil
}

// get attempts to read the requested data from the cache, if it is already
// there.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
func (c *Cache) get(fileNum base.DiskFileNum, p []byte, ofs int64) (n int, _ error) {
	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		shard := c.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - ((ofs + int64(n)) % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		numRead, err := shard.get(fileNum, p[n:n+cappedLen], ofs+int64(n))
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

// set attempts to write the requested data to the cache. Both ofs & len(p) must
// be multiples of the block size.
//
// If all of p is not written to the shard, set returns a non-nil error.
func (c *Cache) set(fileNum base.DiskFileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs%int64(c.blockSize) != 0 || len(p)%c.blockSize != 0 {
			panic(fmt.Sprintf("set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
		}
	}

	// The data extent might cross shard boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		shard := c.getShard(fileNum, ofs+int64(n))
		cappedLen := len(p[n:])
		if toBoundary := int(shardingBlockSize - ((ofs + int64(n)) % shardingBlockSize)); cappedLen > toBoundary {
			cappedLen = toBoundary
		}
		err := shard.set(fileNum, p[n:n+cappedLen], ofs+int64(n))
		if err != nil {
			return err
		}
		// set returns an error if cappedLen bytes aren't written the the shard.
		n += cappedLen
		if n == len(p) {
			// We are done.
			return nil
		}
		// Data extent crosses shard boundary, continue with next shard.
	}
}

func (c *Cache) getShard(fileNum base.DiskFileNum, ofs int64) *shard {
	const prime64 = 1099511628211
	hash := uint64(fileNum.FileNum())*prime64 + uint64(ofs)/shardingBlockSize
	// TODO(josh): Instance change ops are often run in production. Such an operation
	// updates len(c.shards); see openSharedCache. As a result, the behavior of this
	// function changes, and the cache empties out at restart time. We may want a better
	// story here eventually.
	return &c.shards[hash%uint64(len(c.shards))]
}

type shard struct {
	file         vfs.File
	sizeInBlocks int64
	blockSize    int
	mu           struct {
		sync.Mutex
		// TODO(josh): None of these datastructures are space-efficient.
		// Focusing on correctness to start.
		where whereMap
		locks []lockState
		free  []cacheBlockIndex
	}
}

// Maps a logical block in an SST to an index of the cache block with the
// file contents (to the "cache block index").
type whereMap map[logicalBlockID]cacheBlockIndex

type logicalBlockID struct {
	filenum base.DiskFileNum
	// Offset into the file, in units of shard.BlockSize.
	offsetInUnitsOfBlocks int64
}

type cacheBlockIndex int64

type lockState int64

const (
	unlocked lockState = 0
	// >0 lockState tracks the number of distinct readers of some cache block / logical block
	// which is in the secondary cache. It is used to ensure that a cache block is not evicted
	// and overwritten, while there are active readers.
	readLockTakenInc = 1
	// -1 lockState indicates that some cache block is currently being populated with data from
	// blob storage. It is used to ensure that a cache block is not read or evicted again, while
	// it is being populated.
	writeLockTaken = -1
)

func (s *shard) init(
	fs vfs.FS, fsDir string, shardIdx int, sizeInBlocks int64, blockSize int,
) error {
	*s = shard{
		sizeInBlocks: sizeInBlocks,
	}
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

	// TODO(josh): Right now, the secondary cache is not persistent. All existing
	// cache contents will be over-written, since all metadata is only stored in
	// memory.
	s.mu.where = make(whereMap)
	for i := int64(0); i < sizeInBlocks; i++ {
		s.mu.locks = append(s.mu.locks, unlocked)
		s.mu.free = append(s.mu.free, cacheBlockIndex(i))
	}

	return nil
}

func (s *shard) close() error {
	defer func() {
		s.file = nil
	}()
	return s.file.Close()
}

// get attempts to read the requested data from the shard. The data must not
// cross a shard boundary.
//
// If all data is available, returns n = len(p).
//
// If data is partially available, a prefix of the data is read; returns n < len(p)
// and no error. If no prefix is available, returns n = 0 and no error.
//
// TODO(josh): Today, if there are two cache blocks needed to satisfy a read, and the
// first block is not in the cache and the second one is, we will read both from
// blob storage. We should fix this. This is not an unlikely scenario if we are doing
// a reverse scan, since those iterate over sstable blocks in reverse order and due to
// cache block aligned reads will have read the suffix of the sstable block that will
// be needed next.
func (s *shard) get(fileNum base.DiskFileNum, p []byte, ofs int64) (n int, _ error) {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic(fmt.Sprintf("get crosses shard boundary: %v %v", ofs, len(p)))
		}
		s.assertShardStateIsConsistent()
	}

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	for {
		k := logicalBlockID{
			filenum:               fileNum,
			offsetInUnitsOfBlocks: (ofs + int64(n)) / int64(s.blockSize),
		}
		s.mu.Lock()
		cacheBlockInd, ok := s.mu.where[k]
		// TODO(josh): Multiple reads within the same few milliseconds (anything that is smaller
		// than blob storage read latency) that miss on the same logical block ID will not necessarily
		// be rare. We may want to do only one read, with the later readers blocking on the first read
		// completing. This could be implemented either here or in the primary block cache. See
		// https://github.com/cockroachdb/pebble/pull/2586 for additional discussion.
		if !ok {
			s.mu.Unlock()
			return n, nil
		}
		if s.mu.locks[cacheBlockInd] == writeLockTaken {
			// In practice, if we have two reads of the same SST block in close succession, we
			// would expect the second to hit in the in-memory block cache. So it's not worth
			// optimizing this case here.
			s.mu.Unlock()
			return n, nil
		}
		s.mu.locks[cacheBlockInd] += readLockTakenInc
		s.mu.Unlock()

		readAt := int64(cacheBlockInd) * int64(s.blockSize)
		if n == 0 { // if first read
			readAt += ofs % int64(s.blockSize)
		}
		readSize := s.blockSize
		if n == 0 { // if first read
			// Cast to int safe since ofs is modded by block size.
			readSize -= int(ofs % int64(s.blockSize))
		}

		if len(p[n:]) <= readSize {
			numRead, err := s.file.ReadAt(p[n:], readAt)
			s.dropReadLock(cacheBlockInd)
			return n + numRead, err
		}
		numRead, err := s.file.ReadAt(p[n:n+readSize], readAt)
		s.dropReadLock(cacheBlockInd)
		if err != nil {
			return 0, err
		}

		// Note that numRead == readSize, since we checked for an error above.
		n += numRead
	}
}

// set attempts to write the requested data to the shard. The data must not
// cross a shard boundary, and both ofs & len(p) must be multiples of the
// block size.
//
// If all of p is not written to the shard, set returns a non-nil error.
func (s *shard) set(fileNum base.DiskFileNum, p []byte, ofs int64) error {
	if invariants.Enabled {
		if ofs/shardingBlockSize != (ofs+int64(len(p))-1)/shardingBlockSize {
			panic(fmt.Sprintf("set crosses shard boundary: %v %v", ofs, len(p)))
		}
		if ofs%int64(s.blockSize) != 0 || len(p)%s.blockSize != 0 {
			panic(fmt.Sprintf("set with ofs & len not multiples of block size: %v %v", ofs, len(p)))
		}
		s.assertShardStateIsConsistent()
	}

	// The data extent might cross cache block boundaries, hence the loop. In the hot
	// path, max two iterations of this loop will be executed, since reads are sized
	// in units of sstable block size.
	n := 0
	for {
		if n == len(p) {
			return nil
		}
		if invariants.Enabled {
			if n > len(p) {
				panic(fmt.Sprintf("set with n greater than len(p): %v %v", n, len(p)))
			}
		}

		// If the logical block is already in the cache, we should skip doing a set.
		k := logicalBlockID{
			filenum:               fileNum,
			offsetInUnitsOfBlocks: (ofs + int64(n)) / int64(s.blockSize),
		}
		s.mu.Lock()
		if _, ok := s.mu.where[k]; ok {
			s.mu.Unlock()
			n += s.blockSize
			continue
		}

		// Determine cache block index by looking at the free list and randomly evicting if
		// it is empty.
		var cacheBlockInd cacheBlockIndex
		if len(s.mu.free) == 0 {
			// TODO(josh): Right now, we do random eviction. Eventually, we will do something
			// more sophisticated, e.g. leverage ClockPro.
			foundBlockToEvict := false
			var k logicalBlockID
			for k1, v := range s.mu.where {
				// This implies that hot blocks will not be evicted as often, meaning the
				// eviction scheme is not quite random. I think that's fine for now, since we plan
				// to leverage a more sophisticated eviction scheme eventually anyway.
				lock := s.mu.locks[v]
				if lock >= readLockTakenInc || lock == writeLockTaken {
					continue
				}
				foundBlockToEvict = true
				cacheBlockInd = v
				k = k1
				break
			}
			// TODO(josh): We may want to block until a block frees up, instead of returning
			// an error here. But I think we can do that later on, e.g. after running some production
			// experiments.
			if !foundBlockToEvict {
				s.mu.Unlock()
				return errors.New("no block to evict so skipping write to cache")
			}
			delete(s.mu.where, k)
		} else {
			cacheBlockInd = s.mu.free[len(s.mu.free)-1]
			s.mu.free = s.mu.free[:len(s.mu.free)-1]
		}

		s.mu.where[k] = cacheBlockInd
		s.mu.locks[cacheBlockInd] = writeLockTaken
		s.mu.Unlock()

		writeAt := int64(cacheBlockInd) * int64(s.blockSize)

		writeSize := s.blockSize
		if len(p[n:]) <= writeSize {
			writeSize = len(p[n:])
		}

		_, err := s.file.WriteAt(p[n:n+writeSize], writeAt)
		if err != nil {
			s.freeBlock(k, cacheBlockInd)
			return err
		}
		s.dropWriteLock(cacheBlockInd)

		n += writeSize
	}
}

// Doesn't inline currently. This might be okay, but something to keep in mind.
func (s *shard) dropReadLock(cacheBlockInd cacheBlockIndex) {
	s.mu.Lock()
	s.mu.locks[cacheBlockInd] -= readLockTakenInc
	if invariants.Enabled {
		if s.mu.locks[cacheBlockInd] < 0 {
			panic(fmt.Sprintf("unexpected lock state %v in dropReadLock: %v", s.mu.locks[cacheBlockInd], s.mu.locks))
		}
	}
	s.mu.Unlock()
}

// Doesn't inline currently. This might be okay, but something to keep in mind.
func (s *shard) dropWriteLock(cacheBlockInd cacheBlockIndex) {
	s.mu.Lock()
	if invariants.Enabled {
		if s.mu.locks[cacheBlockInd] != writeLockTaken {
			panic(fmt.Sprintf("unexpected lock state %v in dropWriteLock: %v", s.mu.locks[cacheBlockInd], s.mu.locks))
		}
	}
	s.mu.locks[cacheBlockInd] = unlocked
	s.mu.Unlock()
}

// Doesn't inline currently. This might be okay, but something to keep in mind.
func (s *shard) freeBlock(k logicalBlockID, cacheBlockInd cacheBlockIndex) {
	s.mu.Lock()
	delete(s.mu.where, k)
	s.mu.locks[cacheBlockInd] = unlocked
	s.mu.free = append(s.mu.free, cacheBlockInd)
	s.mu.Unlock()
}

func (s *shard) assertShardStateIsConsistent() {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheBlockInds := make(map[cacheBlockIndex]bool)
	for _, v := range s.mu.where {
		if cacheBlockInds[v] {
			panic(fmt.Sprintf("repeated cache block index (where): %v %v %v %v", v, cacheBlockInds, s.mu.where, s.mu.free))
		}
		cacheBlockInds[v] = true
	}
	for _, v := range s.mu.free {
		if cacheBlockInds[v] {
			panic(fmt.Sprintf("repeated cache block index (free): %v %v %v %v", v, cacheBlockInds, s.mu.where, s.mu.free))
		}
		cacheBlockInds[v] = true
	}
	for i := int64(0); i < s.sizeInBlocks; i++ {
		if !cacheBlockInds[cacheBlockIndex(i)] {
			panic(fmt.Sprintf("missing cache block index: %v %v %v %v", i, cacheBlockInds, s.mu.where, s.mu.free))
		}
	}
	if int64(len(s.mu.locks)) != s.sizeInBlocks {
		panic(fmt.Sprintf("lock table isn't correct size: %v %v", len(s.mu.locks), s.sizeInBlocks))
	}
	for _, ls := range s.mu.locks {
		if ls < writeLockTaken {
			panic(fmt.Sprintf("lock state %v is not allowed: %v", ls, s.mu.locks))
		}
	}
}
