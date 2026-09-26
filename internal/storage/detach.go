package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gus-ceraso/Leech/internal/limits"
)

// These narrow seams exercise failures before an original pathname is replaced.
var copyDetachedOutput = copyOutputPrefix
var renameDetachedOutput = os.Rename

// detachOutput replaces a multiply-linked file with a private inode before
// mutation. keep < 0 preserves its complete contents; otherwise only that
// prefix is needed by the impending overwrite or verified truncation.
// The original pathname remains intact until copying and both closes succeed.
func detachOutput(ctx context.Context, path string, keep int64) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: inspect %q for detachment: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("storage: selected output %q is not a regular file", path)
	}
	if !needsDetachment(info) {
		return nil
	}
	length := info.Size()
	if keep >= 0 && keep < length {
		length = keep
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".leech-*")
	if err != nil {
		return fmt.Errorf("storage: detach %q: %w", path, err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if removeErr := os.Remove(tmpPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("storage: remove detached temporary file: %w", removeErr))
		}
	}()
	if length > 0 {
		source, openErr := os.Open(path)
		if openErr != nil {
			return errors.Join(openErr, tmp.Close())
		}
		copyErr := copyDetachedOutput(ctx, tmp, source, length)
		if err := errors.Join(copyErr, source.Close()); err != nil {
			return errors.Join(fmt.Errorf("storage: copy %q for detachment: %w", path, err), tmp.Close())
		}
	}
	modeErr := tmp.Chmod(info.Mode().Perm())
	if err := errors.Join(modeErr, tmp.Close()); err != nil {
		return fmt.Errorf("storage: close detached output %q: %w", path, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := renameDetachedOutput(tmpPath, path); err != nil {
		return fmt.Errorf("storage: replace linked output %q: %w", path, err)
	}
	return nil
}

// Copy exactly the initially observed prefix with one block of memory. A
// growing source cannot extend the work; truncation or a short write fails.
// Resume writes preserve overlong suffixes until verification allows truncation.
func copyOutputPrefix(ctx context.Context, dst io.Writer, src io.Reader, length int64) error {
	buf := make([]byte, limits.BlockBytes)
	for length > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		amount := min(int64(len(buf)), length)
		n, err := io.ReadFull(src, buf[:int(amount)])
		if err != nil {
			return err
		}
		written, err := dst.Write(buf[:n])
		if err != nil {
			return err
		}
		if written != n {
			return io.ErrShortWrite
		}
		length -= int64(n)
	}
	return nil
}
