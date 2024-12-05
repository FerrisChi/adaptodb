package main

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {

	// Store current dir
	testDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}

	// Change to root dir
	if err := os.Chdir(".."); err != nil {
		log.Fatalf("Failed to change to root directory: %v", err)
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}
	log.Printf("Current working directory: %s\n", dir)

	// Run tests
	code := m.Run()

	// Change back to test dir
	if err := os.Chdir(testDir); err != nil {
		log.Printf("Failed to change back to test directory: %v", err)
	}

	os.Exit(code)
}
