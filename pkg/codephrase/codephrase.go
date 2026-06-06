// Package codephrase generates croc-style human code phrases and derives a
// privacy-preserving relay session ID from them.
package codephrase

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
)

// words is a small, unambiguous word list.
var words = []string{
	"brave", "tiger", "comet", "river", "amber", "delta", "ember", "frost",
	"glide", "harbor", "ivory", "jolly", "koala", "lunar", "maple", "noble",
	"ocean", "pearl", "quartz", "raven", "solar", "topaz", "umber", "vivid",
	"willow", "xenon", "yacht", "zephyr", "anchor", "breeze", "cedar", "dune",
	"eagle", "flint", "grove", "hazel", "indigo", "jasper", "kelp", "lotus",
	"mango", "nectar", "onyx", "prism", "quill", "ridge", "spark", "thorn",
	"ultra", "vapor", "wheat", "xerus", "yarrow", "zinc", "azure", "basil",
	"copper", "dusk", "echo", "fable", "garnet", "halo", "iris", "jade",
}

// Generate returns a code phrase like "4-brave-tiger-comet".
func Generate() string {
	digit := randInt(10)
	return fmt.Sprintf("%d-%s-%s-%s",
		digit, words[randInt(len(words))], words[randInt(len(words))], words[randInt(len(words))])
}

// SessionID derives a 16-byte (32 hex char) session ID from a code phrase.
// The relay only ever sees this hash, never the words themselves.
func SessionID(code string) string {
	h := sha256.Sum256([]byte("p2p-session:" + code))
	return hex.EncodeToString(h[:16])
}

func randInt(n int) int {
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		panic(err)
	}
	return int(v.Int64())
}
