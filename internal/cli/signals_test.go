package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestSignalAdapterFirstCancelsAndSecondIsImmediate(t *testing.T) {
	signals := make(chan os.Signal, 2)
	var canceled atomic.Int32
	adapter := newSignalAdapter(signals, func() { canceled.Add(1) }, nil)
	signals <- os.Interrupt
	first, ok := adapter.Wait()
	if !ok || first.Signal != os.Interrupt || first.ExitCode != 130 || first.Immediate {
		t.Fatalf("first event = %#v, ok=%v", first, ok)
	}
	signals <- syscall.SIGTERM
	second, ok := adapter.Wait()
	if !ok || second.Signal != syscall.SIGTERM || second.ExitCode != 143 || !second.Immediate {
		t.Fatalf("second event = %#v, ok=%v", second, ok)
	}
	if got := canceled.Load(); got != 1 {
		t.Fatalf("cancel count = %d, want 1", got)
	}
	adapter.Close()
	if _, ok := adapter.Wait(); ok {
		t.Fatal("closed adapter returned an event")
	}
}

func TestSignalAdapterCloseUnblocksAndDoesNotCancel(t *testing.T) {
	signals := make(chan os.Signal)
	var canceled atomic.Int32
	adapter := newSignalAdapter(signals, func() { canceled.Add(1) }, nil)
	done := make(chan struct{})
	go func() {
		adapter.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not join adapter")
	}
	if canceled.Load() != 0 {
		t.Fatal("closing adapter canceled session")
	}
}

func TestSignalExitCode(t *testing.T) {
	if SignalExitCode(os.Interrupt) != 130 || SignalExitCode(syscall.SIGTERM) != 143 || SignalExitCode(syscall.SIGUSR1) != 1 {
		t.Fatal("unexpected signal exit mapping")
	}
}

func TestSignalAdapterProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM process signaling is not portable to Windows")
	}
	command := exec.Command(os.Args[0], "-test.run=TestSignalAdapterHelperProcess", "--")
	command.Env = append(os.Environ(), "LEECH_SIGNAL_HELPER=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	readLine := func(want string) string {
		t.Helper()
		if !scanner.Scan() {
			t.Fatalf("helper ended before %q: %v; stderr=%q", want, scanner.Err(), stderr.String())
		}
		line := scanner.Text()
		if !strings.HasPrefix(line, want) {
			t.Fatalf("helper line = %q, want prefix %q", line, want)
		}
		return line
	}
	readLine("ready")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	first := readLine("event ")
	if !strings.Contains(first, "code=130 immediate=false") {
		t.Fatalf("first helper event = %q", first)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	second := readLine("event ")
	if !strings.Contains(second, "code=143 immediate=true") {
		t.Fatalf("second helper event = %q", second)
	}
	_ = command.Process.Kill()
	_ = command.Wait()
}

func TestSignalAdapterHelperProcess(t *testing.T) {
	if os.Getenv("LEECH_SIGNAL_HELPER") != "1" {
		return
	}
	adapter := NewSignalAdapter(func() {})
	fmt.Fprintln(os.Stdout, "ready")
	for {
		event, ok := adapter.Wait()
		if !ok {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stdout, "event code=%d immediate=%t signal=%s\n", event.ExitCode, event.Immediate, strconv.Quote(event.Signal.String()))
		if event.Immediate {
			adapter.Close()
			os.Exit(0)
		}
	}
}
