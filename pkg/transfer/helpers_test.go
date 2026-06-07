package transfer

import (
	"crypto/sha256"
	"encoding/hex"
)

// sha256OfBytes is a test helper for computing expected digests.
func sha256OfBytes(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
