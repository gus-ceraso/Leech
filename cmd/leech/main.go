package main

import (
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
	if err := cli.Run(opts, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "leech:", err)
		os.Exit(1)
	}
}
