// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux
// +build linux

package vfs

import "golang.org/x/sys/unix"

func preallocExtend(fd uintptr, offset, length int64) error {
	return unix.Fallocate(int(fd), unix.FALLOC_FL_KEEP_SIZE, offset, length)
}

// Preallocate wraps the Fallocate syscall over a vfs.File
func Preallocate(f File, offset, length int64) error {
	if fdFile, ok := f.(fdGetter); ok {
		return preallocExtend(fdFile.Fd(), offset, length)
	}
	return nil
}
