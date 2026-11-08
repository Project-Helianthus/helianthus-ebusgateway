package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/growattcanclassify"
)

func run(args []string, input io.Reader, output, errors io.Writer) int {
	flags := flag.NewFlagSet("growatt-can-classify", flag.ContinueOnError)
	flags.SetOutput(errors)
	format := flags.String("format", "json", "json or text")
	path := flags.String("input", "-", "local candump text file, or - for stdin")
	mon := flags.String("mon-association", "", "comma-separated interface names with explicit descriptive MON association")
	maxBytes := flags.Int("max-bytes", growattcanclassify.DefaultMaxBytes, "maximum input bytes")
	maxLine := flags.Int("max-line", growattcanclassify.DefaultMaxLine, "maximum line bytes")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || (*format != "json" && *format != "text") {
		_, _ = fmt.Fprintln(errors, "invalid arguments or format")
		return 2
	}
	if *path != "-" {
		info, err := os.Stat(*path)
		if err != nil || !info.Mode().IsRegular() {
			_, _ = fmt.Fprintln(errors, "input must be a regular local text file")
			return 1
		}
		f, err := os.Open(*path)
		if err != nil {
			_, _ = fmt.Fprintln(errors, "cannot open local input")
			return 1
		}
		defer func() { _ = f.Close() }()
		info, err = f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			_, _ = fmt.Fprintln(errors, "input must be a regular local text file")
			return 1
		}
		input = f
	}
	result, err := growattcanclassify.Classify(input, growattcanclassify.Options{MaxBytes: *maxBytes, MaxLine: *maxLine, MONInterfaces: strings.Split(*mon, ",")})
	if err != nil {
		_, _ = fmt.Fprintln(errors, err)
		return 1
	}
	if *format == "json" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(result)
	} else {
		err = growattcanclassify.WriteText(output, result)
	}
	if err != nil {
		_, _ = fmt.Fprintln(errors, "cannot write result")
		return 1
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }
