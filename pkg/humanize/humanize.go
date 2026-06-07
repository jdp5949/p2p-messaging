// Package humanize formats and parses byte sizes, rates, and durations for
// human-friendly CLI output. Units are 1024-based (KB, MB, GB, TB).
package humanize

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Bytes formats a byte count like "512 B", "1.5 KB", "5.0 MB".
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp >= len(units) {
		exp = len(units) - 1
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), units[exp])
}

// Rate formats throughput; returns "—" for non-positive durations.
func Rate(n int64, d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	return Bytes(int64(float64(n)/d.Seconds())) + "/s"
}

// Dur formats a duration as "850ms", "4.2s", or "1m3s".
func Dur(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return fmt.Sprintf("%dm%ds", int(d/time.Minute), int((d%time.Minute)/time.Second))
	}
}

// ParseSize parses "512", "10MB", "1.5GB" (case-insensitive) into bytes.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "KB"):
		mult, s = 1024, strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "MB"):
		mult, s = 1024*1024, strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "GB"):
		mult, s = 1024*1024*1024, strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	s = strings.TrimSpace(s)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("humanize: bad size %q: %w", s, err)
	}
	return int64(f * float64(mult)), nil
}
