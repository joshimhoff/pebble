package vfs

import (
	"github.com/cockroachdb/errors"
	"sync"
)

var _ FS = (*monitoredFS)(nil)
var _ FSWithOpenForWrites = (*monitoredFS)(nil)

type monitoredFS struct {
	FS
	// TODO(): Keep thread-safe but also make fast.
	mu sync.Mutex
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
	BytesRead int
	BytesWritten int
	ReadIops int
	WriteIops int
	// TODO(): Add back.
	//ReadLatency prometheus.Histogram
	//WriteLatency prometheus.Histogram
	//SyncLatency prometheus.Histogram
}

func WithMonitoring(
	innerFS FS,
	stats *Stats,
	) FS {
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
	//now  := time.Now()
	n, err = f.File.Read(p)
	//duration := time.Since(now)

	f.fs.mu.Lock()
	f.fs.stats.BytesRead += len(p)
	f.fs.stats.ReadIops++
	// TODO(): Should we count errors?
	if err != nil {
		// TODO(): Prometheus histograms are already thread-safe, right?
		//f.fs.stats.ReadLatency.Observe(duration.Seconds())
	}
	f.fs.mu.Unlock()

	return n, err
}

func (f *monitoredFile) ReadAt(p []byte, off int64) (n int, err error) {
	//now  := time.Now()
	n, err = f.File.ReadAt(p, off)
	//duration := time.Since(now)

	f.fs.mu.Lock()
	f.fs.stats.BytesRead += len(p)
	f.fs.stats.ReadIops++
	// TODO(): Should we count errors?
	if err != nil {
		//f.fs.stats.ReadLatency.Observe(duration.Seconds())
	}
	f.fs.mu.Unlock()

	return n, err
}

func (f *monitoredFile) Write(p []byte) (n int, err error) {
	//now  := time.Now()
	n, err = f.File.Write(p)
	//duration := time.Since(now)

	f.fs.mu.Lock()
	f.fs.stats.BytesWritten += len(p)
	f.fs.stats.WriteIops++
	// TODO(): Should we count errors?
	if err != nil {
		//f.fs.stats.WriteLatency.Observe(duration.Seconds())
	}
	f.fs.mu.Unlock()

	return n, err
}

func (f *monitoredFile) WriteAt(p []byte, off int64) (n int, err error) {
	randomWriteFile, ok := f.File.(RandomWriteFile)
	if !ok {
		return 0, errors.New("boom")
	}

	//now  := time.Now()
	n, err = randomWriteFile.WriteAt(p, off)
	//duration := time.Since(now)

	f.fs.mu.Lock()
	f.fs.stats.BytesWritten += len(p)
	f.fs.stats.WriteIops++
	// TODO(): Should we count errors?
	if err != nil {
		//f.fs.stats.WriteLatency.Observe(duration.Seconds())
	}
	f.fs.mu.Unlock()

	return n, err

}

func (f *monitoredFile) Sync() (err error) {
	//now  := time.Now()
	err = f.File.Sync()
	//duration := time.Since(now)

	f.fs.mu.Lock()
	// TODO(): Does a sync cost one write iop?
	f.fs.stats.WriteIops++
	// TODO(): Should we count errors?
	if err != nil {
		//f.fs.stats.SyncLatency.Observe(duration.Seconds())
	}
	f.fs.mu.Unlock()

	return err
}
