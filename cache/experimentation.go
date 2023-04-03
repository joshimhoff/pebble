package cache

import (
	"fmt"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/dgryski/go-clockpro"
	"github.com/dgryski/go-s4lru"
	"github.com/dgryski/go-tinylfu"
)

type ReplacementPolicy int

const (
	ClockPro ReplacementPolicy = iota
  S4LRU
	TinyLFU
	// TODO(josh): Implement.
	LRU
)

type Config struct {
	Policy                   ReplacementPolicy
	// If nil, then (i) assume reads & writes will always be in units of pebble sstable
	// block size and (ii) do caching in units of pebble sstable blocks.
	// TODO(josh): I am unsure if the first assumption is okay to make. A related question
	// I have is whether the existing in-memory pebble block cache caches in units of pebble
	// sstable blocks.
	BlockSize                *int64
	CacheSize                int
	// Must be set if Policy == TinyLFU. Else must be nil.
	TinyLFUSamples           *int
	WriteThru                bool
	CacheUserFacingReadsOnly bool
	L5AndL6Only              bool
}

type Results struct {
	Hits   int
	Misses int
}

type Cache interface {
	Get(key string) interface{}
	Set(key string, value interface{})
}

type wrappedS4LRU struct {
	c *s4lru.Cache
}

func (c *wrappedS4LRU) Get(key string) interface{} {
	val, ok := c.c.Get(key)
	if !ok {
		return nil
	}
	return val
}

func (c *wrappedS4LRU) Set(key string, value interface{}) {
	c.c.Set(key, value)
}

type wrappedTinyLFU struct {
	c *tinylfu.T
}

func (c *wrappedTinyLFU) Get(key string) interface{} {
	val, ok := c.c.Get(key)
	if !ok {
		return nil
	}
	return val
}

func (c *wrappedTinyLFU) Set(key string, value interface{}) {
	c.c.Add(key, value)
}

func Simulate(trace []objiotracing.Event, config Config) Results {
	var results Results
	var cache Cache
	if config.Policy == ClockPro {
		cache = clockpro.New(config.CacheSize)
	} else if config.Policy == S4LRU {
		cache = &wrappedS4LRU{s4lru.New(config.CacheSize)}
	} else if config.Policy == TinyLFU {
		if config.TinyLFUSamples == nil {
			panic("samples expected to be set but not set")
		}
		cache = &wrappedTinyLFU{tinylfu.New(config.CacheSize, *config.TinyLFUSamples)}
	} else {
		panic("replacement policy not implemented")
	}
	if config.Policy != TinyLFU && config.TinyLFUSamples != nil {
		panic("sampled expected to not be set but is set")
	}

	for _, e := range trace {
		if config.L5AndL6Only {
			if e.LevelPlusOne <= 5 {
				continue
			}
		}
		if e.Op == objiotracing.ReadOp || e.Op == objiotracing.RecordCacheHitOp {
			if config.CacheUserFacingReadsOnly {
				if e.Reason != objiotracing.UnknownReason {
					continue
				}
			}
			offset := e.Offset
			if config.BlockSize != nil {
				// TODO(josh): The end of a read may hit a different "cache block" than
				// the start of a read. This code currently only simulates reading the
				// first "cache block".
				offset = offset / *config.BlockSize
			}
			k := fmt.Sprintf("%v/%v", e.FileNum, offset)
			v := cache.Get(k)
			if v == nil {
				results.Misses++
				cache.Set(k, true)
			} else  {
				results.Hits++
			}
		}
		if config.WriteThru {
			if e.Op == objiotracing.WriteOp {
				// TODO(josh): The end of a write may hit a different "cache block" than
				// the start of a write. This code currently only simulates writing the
				// first "cache block" out.
				offset := e.Offset
				if config.BlockSize != nil {
					offset = offset / *config.BlockSize
				}
				k := fmt.Sprintf("%v/%v", e.FileNum, e.Offset)
				cache.Set(k, true)
			}
		}
	}

	return results
}
