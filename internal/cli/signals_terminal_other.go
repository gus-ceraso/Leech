//go:build !linux

package cli

import "os"

// Other platforms use the portable character-device check. Platform-specific
// terminal ioctl support can be added here without changing Reporter.
func isTTYFile(file *os.File) bool {
	if file == nil {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
