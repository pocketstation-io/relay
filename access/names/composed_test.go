package names

import (
	"errors"
	"math/big"
	"math/rand"
	"strings"
	"testing"
)

func TestGivenComposedPalettesWhenInspectedThenGroupsAreDisjointCompatibleAndBounded(t *testing.T) {
	owners := map[string]int{}
	for i, p := range composedPalettes {
		for _, w := range strings.Fields(p.modifiers + " " + p.subjects) {
			if _, ok := wordIndex[w]; !ok {
				t.Fatalf("unknown word %s", w)
			}
			if prior, exists := owners[w]; exists {
				t.Fatalf("duplicate word %s in groups%d/%d", w, prior, i)
			}
			owners[w] = i
		}
		if len(composedGroups[i].pairs) == 0 {
			t.Fatalf("empty group%d", i)
		}
		for _, text := range composedGroups[i].pairs {
			if !phrases.admits(strings.Split(text, "-")) {
				t.Fatal("unreviewed component")
			}
		}
	}
	for _, list := range append(append([]string{}, confusableGroups...), disallowedPairs...) {
		owner := -1
		for _, w := range strings.Fields(list) {
			if group, present := owners[w]; present {
				if owner >= 0 && owner != group {
					t.Fatalf("confusable across groups: %s", list)
				}
				owner = group
			}
		}
	}
	if len(composedGroups[0].triples) == 0 {
		t.Fatal("empty initial triple group")
	}
	for count := 4; count <= 15; count++ {
		t.Logf("count=%d capacity=%s", count, CapacityBig(count))
	}
	rng := rand.New(rand.NewSource(128))
	for count := 4; count <= 15; count++ {
		for sample := 0; sample < 2; sample++ {
			name, e := Generate(rng, count)
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("sample%d: %s", count, name.Text)
		}
	}
}
func TestGivenLargeExactCapacityWhenSelectingRanksThenOverflowAndInputOwnershipAreExplicit(t *testing.T) {
	if CapacityBig(15).IsUint64() {
		t.Fatal("largest admitted namespace does not exercise big integer path")
	}
	if _, e := CapacityUint64(15); !errors.Is(e, ErrCapacityOverflow) {
		t.Fatalf("uint64 capacity overflow hidden: %v", e)
	}
	if Capacity(15) != 0 {
		t.Fatal("legacy capacity saturated or truncated")
	}
	prior := CapacityBig(15)
	copy := new(big.Int).Set(prior)
	prior.SetInt64(0)
	if CapacityBig(15).Cmp(copy) != 0 {
		t.Fatal("caller mutated shared capacity")
	}
	for count := 2; count <= 15; count++ {
		capacity := CapacityBig(count)
		last := new(big.Int).Sub(capacity, big.NewInt(1))
		for _, rank := range []*big.Int{big.NewInt(0), big.NewInt(1), last, new(big.Int).Quo(capacity, big.NewInt(2))} {
			saved := new(big.Int).Set(rank)
			name, e := AtBig(rank, count)
			if e != nil {
				t.Fatal(e)
			}
			if rank.Cmp(saved) != 0 {
				t.Fatal("rank mutated")
			}
			if len(strings.Split(name.Text, "-")) != count || len(name.Text) > MaxNameBytes {
				t.Fatal("invalid generated length")
			}
			parsed, e := ParseStored(name.GrammarName, name.Text)
			if e != nil || parsed != name {
				t.Fatalf("round trip count%d: %v", count, e)
			}
		}
		if _, e := AtBig(capacity, count); !errors.Is(e, ErrIndex) {
			t.Fatal("upper rank wrapped")
		}
	}
	for _, rank := range []*big.Int{nil, big.NewInt(-1)} {
		if _, e := AtBig(rank, 15); !errors.Is(e, ErrIndex) {
			t.Fatal("invalid rank accepted")
		}
	}
	for _, count := range []int{-1, 0, 1, 16, 100} {
		if CapacityBig(count).Sign() != 0 {
			t.Fatal("invalid count capacity")
		}
		if _, e := AtBig(big.NewInt(0), count); !errors.Is(e, ErrWordCount) {
			t.Fatal("invalid count accepted")
		}
	}
	for _, grammar := range []string{GrammarName, previousGrammarName, legacyGrammarName} {
		name, _ := AtBig(big.NewInt(0), 15)
		if _, e := ParseStored(grammar, name.Text); e == nil {
			t.Fatal("old grammar accepted new count")
		}
	}
}

func TestGivenMixedRadixNamespaceWhenSamplingAllLengthsThenRanksAreUniqueAndReconstructExactly(t *testing.T) {
	expected := map[int]string{2: "148194", 3: "720927", 4: "4394208", 5: "27955200", 6: "7909574400", 7: "50319360000", 8: "11389787136000", 9: "72459878400000", 10: "20774971736064000", 11: "132166818201600000", 12: "34901952516587520000", 13: "222040254578688000000", 14: "61985867669459435520000", 15: "394343492131749888000000"}
	for count := 2; count <= 15; count++ {
		if CapacityBig(count).String() != expected[count] {
			t.Fatalf("documented exact capacity drifted count%d", count)
		}
	}
	for count := 4; count <= 15; count++ {
		capacity := CapacityBig(count)
		seen := map[string]bool{}
		for sample := int64(0); sample < 1024; sample++ {
			rank := new(big.Int).Quo(new(big.Int).Mul(capacity, big.NewInt(sample)), big.NewInt(1024))
			name, e := AtBig(rank, count)
			if e != nil {
				t.Fatal(e)
			}
			if seen[name.Text] {
				t.Fatal("rank collision")
			}
			seen[name.Text] = true
			parts := strings.Split(name.Text, "-")
			offset := 0
			reconstructed := new(big.Int)
			used := map[string]bool{}
			for _, word := range parts {
				if used[word] {
					t.Fatal("word repeated across groups")
				}
				used[word] = true
			}
			for group := 0; group < count/2; group++ {
				width := 2
				pool, ranks := composedGroups[group].pairs, composedGroups[group].pairRanks
				if group == 0 && count%2 == 1 {
					width = 3
					pool, ranks = composedGroups[group].triples, composedGroups[group].tripleRanks
				}
				local, ok := ranks[strings.Join(parts[offset:offset+width], "-")]
				if !ok {
					t.Fatal("unindexed group")
				}
				reconstructed.Mul(reconstructed, big.NewInt(int64(len(pool))))
				reconstructed.Add(reconstructed, big.NewInt(int64(local)))
				offset += width
			}
			if reconstructed.Cmp(rank) != 0 {
				t.Fatal("mixed-radix rank changed")
			}
		}
		maxBytes := count/2 - 1
		for group := 0; group < count/2; group++ {
			pool := composedGroups[group].pairs
			if group == 0 && count%2 == 1 {
				pool = composedGroups[group].triples
			}
			longest := 0
			for _, text := range pool {
				if len(text) > longest {
					longest = len(text)
				}
			}
			maxBytes += longest
		}
		if maxBytes > MaxNameBytes {
			t.Fatalf("unbounded generated length count%d max%d", count, maxBytes)
		}
	}
	if _, e := Normalize(strings.Repeat("a", MaxNameBytes+1)); e == nil {
		t.Fatal("oversized locator accepted")
	}
	if _, e := Normalize(strings.Repeat("elm-", 15) + "elm"); e == nil {
		t.Fatal("sixteen words accepted")
	}
}
