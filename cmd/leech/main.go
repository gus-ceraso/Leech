package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/gus-ceraso/Leech/internal/cli"
)

func main() {
	opts, err := cli.ParseArgs(os.Args[1:])
	if err != nil {
		var usage *cli.UsageError
		if errors.As(err, &usage) {
			fmt.Fprintf(os.Stderr, "leech: %s\n%s\n", usage, cli.UsageText)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "leech:", err)
		os.Exit(1)
	}
	if opts.Help {
		fmt.Fprint(os.Stdout, cli.HelpText)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	adapter := cli.NewSignalAdapter(cancel)
	err = cli.RunContext(ctx, opts, os.Stdout, os.Stderr)
	var signalEvent cli.SignalEvent
	select {
	case signalEvent = <-adapter.Events():
	default:
	}
	adapter.Close()
	if signalEvent.Signal != nil {
		os.Exit(signalEvent.ExitCode)
	}
	if err != nil {
		os.Exit(1)
	}
}
