package vfs

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var buckets = append(
	prometheus.LinearBuckets(0.0, float64(time.Microsecond*100), 50),
	prometheus.ExponentialBucketsRange(float64(time.Millisecond*5), float64(10*time.Second), 50)...,
)

var _ FS = (*monitoredFS)(nil)
var _ FSWithOpenForWrites = (*monitoredFS)(nil)

type monitoredFS struct {
	FS
	stats *Stats
}

var _ File = (*monitoredFile)(nil)
var _ RandomWriteFile = (*monitoredFile)(nil)

type monitoredFile struct {
	File
	fs *monitoredFS
}

// TODO(): How comprehensive should we be? Should we worry about file system ops?
type Stats struct {
	BytesRead int64
	BytesWritten int64
	ReadIops int64
	WriteIops int64
	ReadErrors int64
	WriteErrors int64
	ReadLatency prometheus.Histogram
	WriteLatency prometheus.Histogram
	SyncLatency prometheus.Histogram
}

func WithMonitoring(
	innerFS FS,
	stats *Stats,
	) FS {
	// TODO(): Fix terrible API.
	stats.ReadLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: buckets,
	})
	stats.WriteLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: buckets,
	})
	stats.SyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Buckets: buckets,
	})
	return &monitoredFS{FS: innerFS, stats: stats}
}

func (fs *monitoredFS) Create(name string) (File, error) {
	file, err := fs.FS.Create(name)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) Open(name string, opts ...OpenOption) (File, error) {
	file, err := fs.FS.Open(name, opts...)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) OpenDir(name string) (File, error) {
	file, err := fs.FS.OpenDir(name)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) ReuseForWrite(oldname, newname string) (File, error) {
	file, err := fs.FS.ReuseForWrite(oldname, newname)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) OpenForWrites(name string, opts ...OpenOption) (RandomWriteFile, error) {
	fsWithOpenForWrites, ok := fs.FS.(FSWithOpenForWrites)
	if !ok {
		return nil, errors.New("blah")
	}
	file, err := fsWithOpenForWrites.OpenForWrites(name, opts...)
	return &monitoredFile{File: file, fs: fs}, err
}

func (f *monitoredFile) Read(p []byte) (n int, err error) {
	now  := time.Now()
	n, err = f.File.Read(p)
	duration := time.Since(now)

	atomic.AddInt64(&f.fs.stats.BytesRead, int64(len(p)))
	atomic.AddInt64(&f.fs.stats.ReadIops, 1)
	if err != nil {
		atomic.AddInt64(&f.fs.stats.ReadErrors, 1)
	} else {
		f.fs.stats.ReadLatency.Observe(float64(duration.Nanoseconds()))
	}

	return n, err
}

func (f *monitoredFile) ReadAt(p []byte, off int64) (n int, err error) {
	now  := time.Now()
	n, err = f.File.ReadAt(p, off)
	duration := time.Since(now)

	atomic.AddInt64(&f.fs.stats.BytesRead, int64(len(p)))
	atomic.AddInt64(&f.fs.stats.ReadIops, 1)
	if err != nil {
		atomic.AddInt64(&f.fs.stats.ReadErrors, 1)
	} else {
		f.fs.stats.ReadLatency.Observe(float64(duration.Nanoseconds()))
	}

	return n, err
}

func (f *monitoredFile) Write(p []byte) (n int, err error) {
	now  := time.Now()
	n, err = f.File.Write(p)
	duration := time.Since(now)

	atomic.AddInt64(&f.fs.stats.BytesWritten, int64(len(p)))
	atomic.AddInt64(&f.fs.stats.WriteIops, 1)
	if err != nil {
		atomic.AddInt64(&f.fs.stats.WriteErrors, 1)
	} else {
		f.fs.stats.WriteLatency.Observe(float64(duration.Nanoseconds()))
	}

	return n, err
}

func (f *monitoredFile) WriteAt(p []byte, off int64) (n int, err error) {
	randomWriteFile, ok := f.File.(RandomWriteFile)
	if !ok {
		return 0, errors.New("boom")
	}

	now  := time.Now()
	n, err = randomWriteFile.WriteAt(p, off)
	duration := time.Since(now)

	atomic.AddInt64(&f.fs.stats.BytesWritten, int64(len(p)))
	atomic.AddInt64(&f.fs.stats.WriteIops, 1)
	if err != nil {
		atomic.AddInt64(&f.fs.stats.WriteErrors, 1)
	} else {
		f.fs.stats.WriteLatency.Observe(float64(duration.Nanoseconds()))
	}

	return n, err

}

func (f *monitoredFile) Sync() (err error) {
	now  := time.Now()
	err = f.File.Sync()
	duration := time.Since(now)

	// TODO(): Does a sync cost one write iop?
	atomic.AddInt64(&f.fs.stats.WriteIops, 1)
	if err != nil {
		atomic.AddInt64(&f.fs.stats.WriteErrors, 1)
	} else {
		f.fs.stats.SyncLatency.Observe(float64(duration.Nanoseconds()))
	}

	return err
}
