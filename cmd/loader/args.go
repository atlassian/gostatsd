package main

import (
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
)

type commandOptions struct {
	Target       string `short:"a" long:"address"                 default:"127.0.0.1:8125" description:"Address to send metrics"                 `
	MetricPrefix string `short:"p" long:"metric-prefix"           default:"loadtest."      description:"Metric name prefix"                      `
	MetricSuffix string `          long:"metric-suffix"           default:".%d"            description:"Metric suffix with cardinality marker"   `
	Rate         uint   `short:"r" long:"rate"                    default:"1000"           description:"Target packets per second"               `
	DatagramSize uint   `          long:"buffer-size"             default:"1500"           description:"Maximum size of datagram to send"        `
	Workers      uint   `short:"w" long:"workers"                 default:"1"              description:"Number of parallel workers to use"       `
	Counts       struct {
		Counter uint64 ` short:"c" long:"counter-count"                                    description:"Number of counters to send"              `
		Gauge   uint64 ` short:"g" long:"gauge-count"                                      description:"Number of gauges to send"                `
		Set     uint64 ` short:"s" long:"set-count"                                        description:"Number of sets to send"                  `
		Timer   uint64 ` short:"t" long:"timer-count"                                      description:"Number of timers to send"                `
	} `group:"Metric count"`
	NameCard struct {
		Counter uint `             long:"counter-cardinality"     default:"1"              description:"Cardinality of counter names"            `
		Gauge   uint `             long:"gauge-cardinality"       default:"1"              description:"Cardinality of gauges names"             `
		Set     uint `             long:"set-cardinality"         default:"1"              description:"Cardinality of set names"                `
		Timer   uint `             long:"timer-cardinality"       default:"1"              description:"Cardinality of timer names"              `
	} `group:"Name cardinality"`
	TagCard struct {
		Counter []uint `           long:"counter-tag-cardinality"                          description:"Cardinality of count tags"               `
		Gauge   []uint `           long:"gauge-tag-cardinality"                            description:"Cardinality of gauge tags"               `
		Set     []uint `           long:"set-tag-cardinality"                              description:"Cardinality of set tags"                 `
		Timer   []uint `           long:"timer-tag-cardinality"                            description:"Cardinality of timer tags"               `
	} `group:"Tag cardinality"`
	ValueRange struct {
		Counter uint `             long:"counter-value-limit"     default:"0"              description:"Maximum value of counters minus one"     `
		Gauge   uint `             long:"gauge-value-limit"       default:"1"              description:"Maximum value of gauges"                 `
		Set     uint `             long:"set-value-cardinality"   default:"1"              description:"Maximum number of values to send per set"`
		Timer   uint `             long:"timer-value-limit"       default:"1"              description:"Maximum value of timers"                 `
	} `group:"Value range"`
}

func parseArgs(args []string) commandOptions {
	var opts commandOptions
	parser := flags.NewParser(&opts, flags.HelpFlag | flags.PassDoubleDash)
	parser.LongDescription = "" + // because gofmt
		"When specifying cardinality, the tag cardinality can be specified multiple times,\n" +
		"and each tag will be named tagN:M.  The maximum total cardinality will be:\n\n" +
		"|name| * |tag1| * |tag2| * ... * |tagN|\n\n" +
		"Care should be taken to not cause a combinatorial explosion."

	positional, err := parser.ParseArgs(args)
	if err != nil {
		if !isHelp(err) {
			parser.WriteHelp(os.Stderr)
			_, _ = fmt.Fprintf(os.Stderr, "\n\nerror parsing command line: %v\n", err)
			os.Exit(1)
		}
		parser.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	if len(positional) != 0 {
		// Near as I can tell there's no way to say no positional arguments allowed.
		parser.WriteHelp(os.Stderr)
		_, _ = fmt.Fprintf(os.Stderr, "\n\nno positional arguments allowed\n")
		os.Exit(1)
	}

	if opts.Counts.Counter+opts.Counts.Gauge+opts.Counts.Set+opts.Counts.Timer == 0 {
		parser.WriteHelp(os.Stderr)
		_, _ = fmt.Fprintf(os.Stderr, "\n\nAt least one of counter-count, gauge-count, set-count, or timer-count must be non-zero\n")
		os.Exit(1)
	}
	return opts
}

// isHelp is a helper to test the error from ParseArgs() to
// determine if the help message was written. It is safe to
// call without first checking that error is nil.
func isHelp(err error) bool {
	// This was copied from https://github.com/jessevdk/go-flags/blame/master/help.go#L499, as there has not been an
	// official release yet with this code. Renamed from WriteHelp to isHelp, as flags.ErrHelp is still returned when
	// flags.HelpFlag is set, flags.PrintError is clear, and -h/--help is passed on the command line, even though the
	// help is not displayed in such a situation.
	if err == nil { // No error
		return false
	}

	flagError, ok := err.(*flags.Error)
	if !ok { // Not a go-flag error
		return false
	}

	if flagError.Type != flags.ErrHelp { // Did not print the help message
		return false
	}

	return true
}
