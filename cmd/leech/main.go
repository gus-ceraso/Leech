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
	defer cancel()
	adapter := cli.NewSignalAdapter(cancel)
	code := runWithSignals(ctx, adapter.Events, adapter.Close, func() error {
		return cli.RunContext(ctx, opts, os.Stdout, os.Stderr)
	}, os.Exit)
	if code != 0 {
		os.Exit(code)
	}
}

func runWithSignals(ctx context.Context, events func() <-chan cli.SignalEvent, closeSignals func(), run func() error, exit func(int)) int {
	received := make(chan cli.SignalEvent, 2)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		for event := range events() {
			if event.Immediate {
				exit(event.ExitCode)
				return
			}
			received <- event
		}
	}()

	err := run()
	closeSignals()
	<-listenerDone

	var event cli.SignalEvent
	if ctx.Err() != nil {
		event = <-received
	} else {
		select {
		case event = <-received:
		default:
		}
	}
	if event.Signal != nil {
		return event.ExitCode
	}
	if err != nil {
		return 1
	}
	return 0
}
