package utils

import "log"

// NamedLogger returns a logging function that prefixes all messages with the given name.
func NamedLogger(name string) func(format string, v ...interface{}) {
	return func(format string, v ...interface{}) {
		prefixedFormat := name + ": " + format
		log.Printf(prefixedFormat, v...)
	}
}
