package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func needsDetachment(info os.FileInfo) bool {
	return info.Sys().(*syscall.Stat_t).Nlink > 1
}

// Linux's pathname syscall limit includes the terminating NUL. Component
// limits are distinct from metainfo's relative-path limits.
func filesystemPath(path string, parts []string) error {
	if len(path) >= syscall.PathMax {
		return fmt.Errorf("output path is not representable: %w", syscall.ENAMETOOLONG)
	}
	parent := path
	for range parts {
		parent = filepath.Dir(parent)
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(parent, &fs); err != nil {
		return fmt.Errorf("inspect output filesystem: %w", err)
	}
	for _, part := range parts {
		if int64(len(part)) > int64(fs.Namelen) {
			return fmt.Errorf("output path component is not representable: %w", syscall.ENAMETOOLONG)
		}
		parent = filepath.Join(parent, part)
		// A preexisting mount can impose a different component limit. Once a
		// directory is absent, its descendants use the last existing filesystem.
		if info, err := os.Lstat(parent); err == nil && info.IsDir() {
			if err := syscall.Statfs(parent, &fs); err != nil {
				return fmt.Errorf("inspect output filesystem: %w", err)
			}
		}
	}
	return nil
}
