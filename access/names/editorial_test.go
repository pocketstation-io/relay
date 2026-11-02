package names

import (
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestGivenReviewedLexiconWhenMeasuredThenCapacityAndSeededSamplesRemainInspectable(t *testing.T) {
	n, a, m := 0, 0, 0
	var unused []string
	for i, r := range phrases.roles {
		if r.noun != 0 {
			n++
		}
		if r.adjective != 0 {
			a++
		}
		if r.modifier != 0 {
			m++
		}
		if r.noun|r.adjective|r.modifier == 0 {
			unused = append(unused, words[i])
		}
	}
	t.Logf("pool=%d subjects=%d adjectives=%d noun_modifiers=%d legacy_only=%d capacity2=%d capacity3=%d blocks2=%d blocks3=%d index_bytes=%d", len(words), n, a, m, len(unused), Capacity(2), Capacity(3), len(phrases.pairs), len(phrases.triples), uintptr(cap(phrases.pairs)+cap(phrases.triples))*unsafe.Sizeof(phraseBlock{}))
	t.Logf("legacy_only: %s", strings.Join(unused, " "))
	rng := rand.New(rand.NewSource(127))
	for _, count := range []int{2, 3} {
		var samples []string
		for i := 0; i < 64; i++ {
			name, err := Generate(rng, count)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, name.Text)
		}
		t.Logf("samples%d: %s", count, strings.Join(samples, ", "))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	g := buildPhraseGrammar()
	runtime.ReadMemStats(&after)
	t.Logf("build_elapsed=%s allocated_bytes=%d retained_index_capacity=%d", time.Since(start), after.TotalAlloc-before.TotalAlloc, cap(g.triples))
	runtime.KeepAlive(g)
}
