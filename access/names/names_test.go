package names

import (
	"bytes"
	"errors"
	"io"
	"math/bits"
	"strings"
	"testing"
)

func TestGivenCuratedVocabularyWhenInspectedThenWordsAreShortDistinctAndURLSafe(t *testing.T) {
	approvedLong := map[string]bool{}
	for _, word := range strings.Fields("apricot balance blanket cricket daffodil drizzle elegant flannel granite hexagon jasmine leather mountain penguin saffron thunder vanilla village") {
		approvedLong[word] = true
	}
	vocabularies := Vocabulary()
	for slot, pool := range vocabularies {
		seen := map[string]bool{}
		for index, word := range pool {
			if len(word) < 3 || (len(word) > 6 && !approvedLong[word]) || seen[word] {
				t.Fatalf("word length or duplication: %q", word)
			}
			seen[word] = true
			if word != vocabularies[0][index] {
				t.Fatalf("slot %d has a different pool", slot)
			}
			for _, character := range word {
				if character < 'a' || character > 'z' {
					t.Fatalf("non-URL-safe word %q", word)
				}
			}
		}
	}
	for word := range approvedLong {
		if _, ok := wordIndex[word]; !ok {
			t.Fatalf("approved long word missing: %s", word)
		}
	}
	vocabulary := Vocabulary()
	vocabulary[0][0] = "mutated"
	name, err := At(0, 2)
	if err != nil || name.Text != "acorn-arch" {
		t.Fatalf("caller changed generation: %v %v", name, err)
	}
}

func TestGivenTwoWordNamespaceWhenEnumeratedThenEveryNameIsUniqueAndRoundTrips(t *testing.T) {
	seen := make([]bool, len(words)*len(words))
	checked := uint64(0)
	for index := uint64(0); index < Capacity(2); index++ {
		name, err := At(index, 2)
		if err != nil {
			t.Fatalf("duplicate or unavailable index %d: %v", index, err)
		}
		parts := strings.Split(name.Text, "-")
		if len(parts) != 2 {
			t.Fatalf("invalid pair %q", name.Text)
		}
		first, firstOK := wordIndex[parts[0]]
		second, secondOK := wordIndex[parts[1]]
		if !firstOK || !secondOK || first == second {
			t.Fatalf("invalid words %q", name.Text)
		}
		key := first*len(words) + second
		if seen[key] {
			t.Fatalf("duplicate pair %q", name.Text)
		}
		seen[key] = true
		checked++
		parsed, err := Parse(name.Text)
		if err != nil || parsed != name || name.GrammarName != GrammarName {
			t.Fatalf("name does not round trip: %v %v", name, err)
		}
	}
	if checked != Capacity(2) {
		t.Fatal("capacity includes duplicate names")
	}
}

func TestGivenThreeWordNamespaceWhenSamplingBoundariesThenNamesRoundTripWithoutVersionPrefix(t *testing.T) {
	indices := []uint64{0, 1, uint64(len(words) - 2), uint64(len(words) - 1), Capacity(3) - 1}
	for sample := uint64(0); sample < 4096; sample++ {
		indices = append(indices, sample*Capacity(3)/4096)
	}
	for i, block := range phrases.triples {
		indices = append(indices, block.end-1)
		if i > 0 {
			indices = append(indices, phrases.triples[i-1].end)
		}
	}
	for _, index := range indices {
		name, err := At(index, 3)
		if err != nil || len(name.Text) > 25 || strings.Contains(name.Text, GrammarName) {
			t.Fatalf("invalid displayed name: %v %v", name, err)
		}
		parsed, err := Parse("  " + strings.ToUpper(name.Text) + "  ")
		if err != nil || parsed != name {
			t.Fatalf("normalization changed identity: %v %v", parsed, err)
		}
	}
}

func TestGivenRejectedRandomSampleWhenGeneratingThenSamplingRetriesWithoutModuloBias(t *testing.T) {
	// A maximal masked sample lies beyond the non-power-of-two capacity;
	// rejection consumes another sample, while modulo mapping would not.
	bytesPerSample := (bits.Len64(Capacity(2)-1) + 7) / 8
	reader := bytes.NewReader(append(bytes.Repeat([]byte{255}, bytesPerSample), make([]byte, bytesPerSample)...))
	name, err := Generate(reader, 2)
	if err != nil || name.Text != "acorn-arch" || reader.Len() != 0 {
		t.Fatalf("rejection sampling failed: %v %v remaining=%d", name, err, reader.Len())
	}
}

func TestGivenInvalidInputWhenGeneratingOrParsingThenErrorsDoNotProduceNames(t *testing.T) {
	for _, count := range []int{-1, 0, 1, 16} {
		if Capacity(count) != 0 {
			t.Fatalf("invalid capacity for %d", count)
		}
		if _, err := Generate(bytes.NewReader(nil), count); !errors.Is(err, ErrWordCount) {
			t.Fatalf("invalid word count accepted: %v", err)
		}
	}
	if _, err := At(Capacity(2), 2); !errors.Is(err, ErrIndex) {
		t.Fatal("index wrapped into a valid name")
	}
	if _, err := Generate(bytes.NewReader(nil), 2); !errors.Is(err, io.EOF) {
		t.Fatalf("randomness failure hidden: %v", err)
	}
	if _, err := Generate(nil, 2); err == nil {
		t.Fatal("missing randomness accepted")
	}
	for _, input := range []string{"quiet", "quiet-elm-vale-bay", "quiet/elm", "quiet--elm", "quiet-elm?token=x", "quiet-élm", "en-scene-v1", "unknown-unknown", strings.Repeat(" ", 129)} {
		if _, err := Parse(input); !errors.Is(err, ErrName) {
			t.Fatalf("invalid name accepted: %q, %v", input, err)
		}
	}
}

func TestGivenLegacyGrantNameWhenParsedThenVersionIsPreservedWithoutNewLegacyGeneration(t *testing.T) {
	for _, text := range []string{"amberaura-amberbadger", "wanderingradiance-tranquilsparrow-tranquilprairie"} {
		name, err := Parse(text)
		if err != nil || name.GrammarName != legacyGrammarName || name.Text != text {
			t.Fatalf("legacy lookup broken: %v %v", name, err)
		}
	}
	if _, err := Parse("amberaura-acorn"); err == nil {
		t.Fatal("mixed legacy and current grammar accepted")
	}
	name, err := Generate(bytes.NewReader(make([]byte, 8)), 3)
	if err != nil || name.GrammarName != GrammarName || name.Text != "amber-acorn-arch" {
		t.Fatalf("new allocation used legacy grammar: %v %v", name, err)
	}
}

func TestGivenReviewedPhrasesWhenParsedThenOnlyAdmittedPatternsUseCurrentGrammar(t *testing.T) {
	for _, text := range []string{"quiet-orbit", "rice-river", "silly-mountain", "lemon-corpus", "velvet-moon", "quiet-rice-river", "soft-velvet-moon", "pale-blue-moon"} {
		if n, e := ParseStored(GrammarName, text); e != nil || n.GrammarName != GrammarName {
			t.Fatalf("reviewed phrase %q rejected: %v", text, e)
		}
	}
	for _, text := range []string{"mountain-silly-lemon", "candid-larch-frost", "wise-flannel-bath", "agile-rink", "easy-mound", "ready-rain-box", "brief-dew-teapot", "rice-rise", "beech-beach", "pearly-pear-harbor", "golden-gold-garden", "snowy-snow-river", "high-leather-earth", "wide-petal-sun", "sheer-fig-haven", "pastel-ochre-haven", "solar-navy-haven", "blue-green-moon", "rice-rice"} {
		if _, e := ParseStored(GrammarName, text); e == nil {
			t.Fatalf("ineligible current phrase: %s", text)
		}
	}
	for _, text := range []string{"mountain-silly-lemon", "pearly-pear-harbor", "acorn-agate", "daffodil-mountain-apricot", "rice-river"} {
		n, e := ParseStored(previousGrammarName, text)
		if e != nil || n.GrammarName != previousGrammarName {
			t.Fatalf("frozen word grammar lost: %s %v", text, e)
		}
	}
	if _, e := ParseStored("unknown", "rice-river"); e == nil {
		t.Fatal("unknown stored grammar accepted")
	}
}

func TestGivenEditorialDataWhenIndexedThenEveryRoleReferencesFrozenVocabulary(t *testing.T) {
	for _, groups := range [][]lexicalGroup{nounGroups, adjectiveGroups, modifierGroups} {
		for _, group := range groups {
			for _, word := range strings.Fields(group.words) {
				if _, ok := wordIndex[word]; !ok {
					t.Fatalf("unknown lexical word %s", word)
				}
			}
		}
	}
	for _, list := range append(append(append([]string{}, qualifierPairs...), confusableGroups...), disallowedPairs...) {
		for _, word := range strings.Fields(list) {
			if _, ok := wordIndex[word]; !ok {
				t.Fatalf("unknown compatibility word %s", word)
			}
		}
	}
	for adjective, heads := range specificAdjectiveHeads {
		for _, word := range strings.Fields(adjective + " " + heads) {
			if _, ok := wordIndex[word]; !ok {
				t.Fatalf("unknown specific adjective/head %s", word)
			}
		}
	}
	for _, word := range strings.Fields(tripleHeads + " " + tripleAdjectives) {
		if _, ok := wordIndex[word]; !ok {
			t.Fatalf("unknown triple word %s", word)
		}
	}
	if len(words) != 1023 || (len(words)+63)/64 != bitsetWords {
		t.Fatal("frozen vocabulary changed")
	}
	for _, excluded := range []string{"pansy", "knob", "chest", "organ", "token"} {
		if _, exists := wordIndex[excluded]; exists {
			t.Fatal("excluded vocabulary returned")
		}
	}
	// Build counts are a union, not a sum of overlapping role combinations.
	var pairs, triples uint64
	for a := range words {
		pairs += phrases.heads(a, -1).count()
		for b := range words {
			triples += phrases.heads(a, b).count()
		}
	}
	if Capacity(2) != pairs || Capacity(3) != triples {
		t.Fatal("admissibility and indexed capacities differ")
	}
}
