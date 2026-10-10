// Package names supplies short readable names for Relay join links. A name is
// a locator, never a credential. The access service owns atomic reservation.
package names

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
)

// GrammarName is stored with grants for migration. It is not part of a URL.
const GrammarName = "natural-phrases-1"

const previousGrammarName = "natural-words-1"

const legacyGrammarName = "en-scene-v1"

var (
	ErrWordCount        = errors.New("readable names require 2 through 15 words")
	ErrCapacityOverflow = errors.New("readable name capacity exceeds uint64; use CapacityBig")
	ErrName             = errors.New("unrecognized readable name")
	ErrIndex            = errors.New("readable name index out of range")
)

type Name struct {
	Text        string
	GrammarName string
	WordCount   int
}

// Capacity is the exact number of editorially admitted phrases. It is not
// authorization strength or the number of Sessions a service can support.
// Deprecated: use CapacityBig or CapacityUint64. This compatibility function
// returns zero for invalid counts OR uint64 overflow; it never saturates.
func Capacity(wordCount int) uint64 {
	n, err := CapacityUint64(wordCount)
	if err != nil {
		return 0
	}
	return n
}

// Generate uses rejection sampling so unequal vocabulary sizes remain unbiased.
// It performs no reservation; callers must atomically reject occupied names.
func Generate(random io.Reader, wordCount int) (Name, error) {
	capacity := CapacityBig(wordCount)
	if capacity.Sign() == 0 {
		return Name{}, ErrWordCount
	}
	if random == nil {
		return Name{}, errors.New("readable names require a randomness source")
	}
	index, err := rand.Int(random, capacity)
	if err != nil {
		return Name{}, fmt.Errorf("generate readable name: %w", err)
	}
	return AtBig(index, wordCount)
}

// At enumerates names without collisions for diagnostics and caller-limited
// allocation attempts. An index at or beyond CapacityBig is rejected, not wrapped.
// Indices are not durable identities: vocabulary growth changes their mapping.
func At(index uint64, wordCount int) (Name, error) {
	return AtBig(new(big.Int).SetUint64(index), wordCount)
}

// Normalize accepts the syntax of retained legacy names as well as new names.
// It does not prove that a grant exists or that the caller may redeem it.
func Normalize(text string) (string, error) {
	if len(text) > MaxNameBytes {
		return "", ErrName
	}
	value := strings.ToLower(strings.TrimSpace(text))
	words := strings.Split(value, "-")
	if len(words) < MinWordCount || len(words) > MaxWordCount {
		return "", ErrName
	}
	for _, word := range words {
		if len(word) < 3 || len(word) > 24 {
			return "", ErrName
		}
		for _, character := range word {
			if character < 'a' || character > 'z' {
				return "", ErrName
			}
		}
	}
	return value, nil
}

// Parse recognizes current phrases and both frozen previous grammars. Storage
// readers use ParseStored to preserve the recorded grammar without reclassifying.
func Parse(text string) (Name, error) {
	for _, grammar := range []string{GrammarName, ComposedGrammarName, previousGrammarName, legacyGrammarName} {
		if name, err := ParseStored(grammar, text); err == nil {
			return name, nil
		}
	}
	return Name{}, ErrName
}

// ParseStored validates a name against its recorded grammar. It never upgrades
// a stored name or interprets the name as a credential.
func ParseStored(grammar, text string) (Name, error) {
	value, err := Normalize(text)
	if err != nil {
		return Name{}, err
	}
	parts := strings.Split(value, "-")
	valid := false
	switch grammar {
	case GrammarName:
		valid = (len(parts) == 2 || len(parts) == 3) && phrases.admits(parts)
	case ComposedGrammarName:
		valid = parseComposed(parts)
	case previousGrammarName:
		valid = len(parts) == 2 || len(parts) == 3
		for i, w := range parts {
			if _, ok := wordIndex[w]; !ok || contains(parts[:i], w) {
				valid = false
			}
		}
	case legacyGrammarName:
		valid = (len(parts) == 2 || len(parts) == 3) && legacyWord(parts[0], legacyModifierRoots, legacyModifierTails) && legacyWord(parts[1], legacySubjectRoots, legacySubjectTails) && (len(parts) == 2 || legacyWord(parts[2], legacyPlaceRoots, legacyPlaceTails))
	}
	if !valid {
		return Name{}, ErrName
	}
	return Name{Text: value, GrammarName: grammar, WordCount: len(parts)}, nil
}

// Vocabulary retains the historical three-copy inspection shape; it does not
// describe the positional palettes of composed names. It returns copies without permitting
// callers to change generation or parsing globally. This frozen lexical pool
// includes words retained only for old links; editorial roles govern generation.
func Vocabulary() [][]string {
	return [][]string{append([]string(nil), words...), append([]string(nil), words...), append([]string(nil), words...)}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func legacyWord(word string, roots, tails []string) bool {
	for _, root := range roots {
		if strings.HasPrefix(word, root) && contains(tails, strings.TrimPrefix(word, root)) {
			return true
		}
	}
	return false
}

// Built once at package initialization and never mutated. Public vocabulary
// access returns copies, so callers cannot alter parsing or generation.
var wordIndex = func() map[string]int {
	index := make(map[string]int, len(words))
	for position, word := range words {
		index[word] = position
	}
	return index
}()
