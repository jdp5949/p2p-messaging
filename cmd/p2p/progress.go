package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/jdp5949/p2p-messaging/pkg/humanize"
)

// formatProgress renders a single-line progress string. total==0 => unknown
// size, shows a running byte count.
func formatProgress(done, total int64) string {
	if total > 0 {
		pct := float64(done) / float64(total) * 100
		bars := int(pct / 5)
		if bars > 20 {
			bars = 20
		}
		return fmt.Sprintf("[%-20s] %5.1f%% (%s / %s)", strings.Repeat("=", bars), pct,
			humanize.Bytes(done), humanize.Bytes(total))
	}
	return humanize.Bytes(done)
}

// progressBar prints formatProgress to stderr in place.
func progressBar(done, total int64) {
	fmt.Fprintf(os.Stderr, "\r%s", formatProgress(done, total))
}

// promptOverwrite asks the user whether to overwrite name.
func promptOverwrite(name string) bool {
	fmt.Fprintf(os.Stderr, "overwrite %s? [y/N] ", name)
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}
