package vfs

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var _ FS = (*monitoredFS)(nil)

type monitoredFS struct {
	FS
	stats *Stats
}

var _ File = (*monitoredFile)(nil)

type monitoredFile struct {
	File
	fs *monitoredFS
}

// TODO(): How comprehensive should we be? Should we worry about file system ops?
type Stats struct {
	BytesRead int
	BytesWritten int
	ReadIops int
	WriteIops int
	ReadLatency prometheus.Histogram
	WriteLatency prometheus.Histogram
	SyncLatency prometheus.Histogram
}

func WithMonitoring(
	innerFS FS,
	stats *Stats,
	) FS {
	return &monitoredFS{FS: innerFS, stats: stats}
}

func (fs *monitoredFS) Create(name string) (File, error) {
	file, err := fs.Create(name)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) Open(name string, opts ...OpenOption) (File, error) {
	file, err := fs.Open(name, opts...)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) OpenDir(name string) (File, error) {
	file, err := fs.OpenDir(name)
	return &monitoredFile{File: file, fs: fs}, err
}

func (fs *monitoredFS) ReuseForWrite(oldname, newname string) (File, error) {
	file, err := fs.ReuseForWrite(oldname, newname)
	return &monitoredFile{File: file, fs: fs}, err
}

func (f *monitoredFile) Read(p []byte) (n int, err error) {
	now  := time.Now()
	n, err = f.File.Read(p)
	duration := time.Since(now)

	f.fs.stats.BytesRead += len(p)
	f.fs.stats.ReadIops++
	// TODO(): Should we count errors?
	if err != nil {
		f.fs.stats.ReadLatency.Observe(duration.Seconds())
	}

	return n, err
}

func (f *monitoredFile) ReadAt(p []byte, off int64) (n int, err error) {
	now  := time.Now()
	n, err = f.File.ReadAt(p, off)
	duration := time.Since(now)

	f.fs.stats.BytesRead += len(p)
	f.fs.stats.ReadIops++
	// TODO(): Should we count errors?
	if err != nil {
		f.fs.stats.ReadLatency.Observe(duration.Seconds())
	}

	return n, err
}

func (f *monitoredFile) Write(p []byte) (n int, err error) {
	now  := time.Now()
	n, err = f.File.Write(p)
	duration := time.Since(now)

	f.fs.stats.BytesWritten += len(p)
	f.fs.stats.WriteIops++
	// TODO(): Should we count errors?
	if err != nil {
		f.fs.stats.WriteLatency.Observe(duration.Seconds())
	}

	return n, err
}

func (f *monitoredFile) Sync() (err error) {
	now  := time.Now()
	err = f.File.Sync()
	duration := time.Since(now)

	// TODO(): Does a sync cost one write iop?
	f.fs.stats.WriteIops++
	// TODO(): Should we count errors?
	if err != nil {
		f.fs.stats.SyncLatency.Observe(duration.Seconds())
	}

	return err
}
