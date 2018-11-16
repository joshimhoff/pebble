// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/sstable"
	"github.com/petermattis/pebble/storage"
)

type tableCache struct {
	dirname string
	fs      storage.Storage
	opts    *db.Options
	size    int

	mu struct {
		sync.Mutex
		cond      sync.Cond
		nodes     map[uint64]*tableCacheNode
		dummy     tableCacheNode
		releasing int
	}
}

func (c *tableCache) init(dirname string, fs storage.Storage, opts *db.Options, size int) {
	c.dirname = dirname
	c.fs = fs
	c.opts = opts
	c.size = size
	c.mu.cond.L = &c.mu.Mutex
	c.mu.nodes = make(map[uint64]*tableCacheNode)
	c.mu.dummy.next = &c.mu.dummy
	c.mu.dummy.prev = &c.mu.dummy
}

func (c *tableCache) newIter(meta *fileMetadata) (db.InternalIterator, error) {
	// Calling findNode gives us the responsibility of decrementing n's
	// refCount. If opening the underlying table resulted in error, then we
	// decrement this straight away. Otherwise, we pass that responsibility
	// to the tableCacheIter, which decrements when it is closed.
	n := c.findNode(meta)
	x := <-n.result
	if x.err != nil {
		c.mu.Lock()
		n.refCount--
		if n.refCount == 0 {
			c.mu.releasing++
			go n.release(c)
		}
		c.mu.Unlock()

		// Try loading the table again; the error may be transient.
		go n.load(c)
		return nil, x.err
	}
	n.result <- x

	iter := x.reader.NewIter(nil)
	iter.(*sstable.Iter).SetCloseHook(func() error {
		c.mu.Lock()
		n.refCount--
		if n.refCount == 0 {
			c.mu.releasing++
			go n.release(c)
		}
		c.mu.Unlock()
		return nil
	})
	return iter, nil
}

// releaseNode releases a node from the tableCache.
//
// c.mu must be held when calling this.
func (c *tableCache) releaseNode(n *tableCacheNode) {
	delete(c.mu.nodes, n.meta.fileNum)
	n.next.prev = n.prev
	n.prev.next = n.next
	n.refCount--
	if n.refCount == 0 {
		c.mu.releasing++
		go n.release(c)
	}
}

// findNode returns the node for the table with the given file number, creating
// that node if it didn't already exist. The caller is responsible for
// decrementing the returned node's refCount.
func (c *tableCache) findNode(meta *fileMetadata) *tableCacheNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.mu.nodes[meta.fileNum]
	if n == nil {
		n = &tableCacheNode{
			meta:     meta,
			refCount: 1,
			result:   make(chan tableReaderOrError, 1),
		}
		c.mu.nodes[meta.fileNum] = n
		if len(c.mu.nodes) > c.size {
			// Release the tail node.
			c.releaseNode(c.mu.dummy.prev)
		}
		go n.load(c)
	} else {
		// Remove n from the doubly-linked list.
		n.next.prev = n.prev
		n.prev.next = n.next
	}
	// Insert n at the front of the doubly-linked list.
	n.next = c.mu.dummy.next
	n.prev = &c.mu.dummy
	n.next.prev = n
	n.prev.next = n
	// The caller is responsible for decrementing the refCount.
	n.refCount++
	return n
}

func (c *tableCache) evict(fileNum uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n := c.mu.nodes[fileNum]; n != nil {
		c.releaseNode(n)
	}
}

func (c *tableCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for n := c.mu.dummy.next; n != &c.mu.dummy; n = n.next {
		n.refCount--
		if n.refCount == 0 {
			c.mu.releasing++
			go n.release(c)
		}
	}
	c.mu.nodes = nil
	c.mu.dummy.next = nil
	c.mu.dummy.prev = nil

	for c.mu.releasing > 0 {
		c.mu.cond.Wait()
	}
	return nil
}

type tableReaderOrError struct {
	reader *sstable.Reader
	err    error
}

type tableCacheNode struct {
	meta   *fileMetadata
	result chan tableReaderOrError

	// The remaining fields are protected by the tableCache mutex.

	next, prev *tableCacheNode
	refCount   int
}

func (n *tableCacheNode) load(c *tableCache) {
	// Try opening the fileTypeTable first.
	f, err := c.fs.Open(dbFilename(c.dirname, fileTypeTable, n.meta.fileNum))
	if err != nil {
		n.result <- tableReaderOrError{err: err}
		return
	}
	r := sstable.NewReader(f, n.meta.fileNum, c.opts)
	if n.meta.smallestSeqNum == n.meta.largestSeqNum {
		r.Properties.GlobalSeqNum = n.meta.largestSeqNum
	}
	n.result <- tableReaderOrError{reader: r}
}

func (n *tableCacheNode) release(c *tableCache) {
	x := <-n.result
	if x.err == nil {
		// Nothing to be done about an error at this point.
		_ = x.reader.Close()
	}
	c.mu.Lock()
	c.mu.releasing--
	c.mu.Unlock()
	c.mu.cond.Signal()
}
