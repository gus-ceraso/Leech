//go:build linux

package cli

import (
	"os"
	"syscall"
	"unsafe"
)

// isTTYFile asks the kernel whether the descriptor implements the terminal
// ioctl. Stat mode alone is insufficient: /dev/null is a character device but
// must not receive replaceable status output.
func isTTYFile(file *os.File) bool {
	if file == nil {
		return false
	}
	var termios syscall.Termios
	_, _, errno := syscall.Syscall6(
		syscall.SYS_IOCTL,
		file.Fd(),
		uintptr(syscall.TCGETS),
		uintptr(unsafe.Pointer(&termios)),
		0,
		0,
		0,
	)
	return errno == 0
}
