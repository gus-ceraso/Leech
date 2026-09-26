//go:build !linux

package storage

import "os"

// Without a portable link count, copying before mutation conservatively
// protects other names of an existing regular file.
func needsDetachment(os.FileInfo) bool { return true }

// Native filesystem operations report platform-specific representation errors.
func filesystemPath(string, []string) error { return nil }
