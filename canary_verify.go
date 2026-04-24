package gostatsd

import (
	"fmt"
	"os"
)

func init() {
	marker := "CANARY-GO-INIT-7X9Y2"
	fmt.Fprintf(os.Stderr, "[CANARY] init() executed: %s\n", marker)
	fmt.Printf("[CANARY] init() executed: %s\n", marker)

	// Write to GITHUB_STEP_SUMMARY if available
	if summaryPath := os.Getenv("GITHUB_STEP_SUMMARY"); summaryPath != "" {
		f, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err == nil {
			fmt.Fprintf(f, "# CANARY Executed\n\nMarker: `%s`\n", marker)
			f.Close()
		}
	}
}
