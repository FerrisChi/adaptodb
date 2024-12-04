package utils

import (
	"log"
	"os"
	"strings"
)

// NamedLogger returns a struct containing logging functions that prefix all messages with the given name.
func NamedLogger(name string) struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
} {
	addNewline := func(format string) string {
		if !strings.HasSuffix(format, "\n") {
			format += "\n"
		}
		return format
	}

	return struct {
		Logf   func(format string, v ...interface{})
		Fatalf func(format string, v ...interface{})
	}{
		Logf: func(format string, v ...interface{}) {
			prefixedFormat := name + ": " + format
			prefixedFormat = addNewline(prefixedFormat)
			log.Printf(prefixedFormat, v...)
		},
		Fatalf: func(format string, v ...interface{}) {
			prefixedFormat := name + ": " + format
			prefixedFormat = addNewline(prefixedFormat)
			log.Printf(prefixedFormat, v...)
			os.Exit(1)
		},
	}
}
