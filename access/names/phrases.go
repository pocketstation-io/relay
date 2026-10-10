package names

import (
	"math/bits"
	"sort"
	"strings"
)

const MaxPhraseBytes = 24
const bitsetWords = (1023 + 63) / 64

type wordSet [bitsetWords]uint64

func (s *wordSet) add(i int)     { s[i/64] |= 1 << uint(i%64) }
func (s wordSet) has(i int) bool { return s[i/64]&(1<<uint(i%64)) != 0 }
func (s wordSet) count() uint64 {
	var n uint64
	for _, w := range s {
		n += uint64(bits.OnesCount64(w))
	}
	return n
}
func (s wordSet) at(rank uint64) int {
	for slot, w := range s {
		n := uint64(bits.OnesCount64(w))
		if rank >= n {
			rank -= n
			continue
		}
		for rank > 0 {
			w &= w - 1
			rank--
		}
		return slot*64 + bits.TrailingZeros64(w)
	}
	return -1
}

type lexicalRole struct{ noun, adjective, modifier family }
type phraseBlock struct {
	first, second uint16
	heads         wordSet
	end           uint64
}
type phraseGrammar struct {
	roles                                     []lexicalRole
	compatible, adjectiveHeads, modifierHeads []wordSet
	qualifiers                                map[[2]int]bool
	tripleHeads, tripleAdjectives             wordSet
	pairs, triples                            []phraseBlock
	lengthMasks                               [MaxPhraseBytes + 1]wordSet
}

var phrases = buildPhraseGrammar()

func buildPhraseGrammar() phraseGrammar {
	g := phraseGrammar{roles: make([]lexicalRole, len(words)), compatible: make([]wordSet, len(words)), adjectiveHeads: make([]wordSet, len(words)), modifierHeads: make([]wordSet, len(words)), qualifiers: map[[2]int]bool{}}
	for _, group := range nounGroups {
		for _, w := range strings.Fields(group.words) {
			i := wordIndex[w]
			g.roles[i].noun |= group.family
		}
	}
	for _, group := range adjectiveGroups {
		for _, w := range strings.Fields(group.words) {
			i := wordIndex[w]
			g.roles[i].adjective |= group.family
		}
	}
	for _, group := range modifierGroups {
		for _, w := range strings.Fields(group.words) {
			i := wordIndex[w]
			g.roles[i].modifier |= group.family
		}
	}
	blocked := map[[2]int]bool{}
	for _, group := range confusableGroups {
		list := strings.Fields(group)
		for _, a := range list {
			for _, b := range list {
				blocked[[2]int{wordIndex[a], wordIndex[b]}] = true
			}
		}
	}
	for _, pair := range disallowedPairs {
		p := strings.Fields(pair)
		a, b := wordIndex[p[0]], wordIndex[p[1]]
		blocked[[2]int{a, b}] = true
		blocked[[2]int{b, a}] = true
	}
	for _, pair := range qualifierPairs {
		p := strings.Fields(pair)
		g.qualifiers[[2]int{wordIndex[p[0]], wordIndex[p[1]]}] = true
	}
	for limit := 0; limit <= MaxPhraseBytes; limit++ {
		for i, w := range words {
			if len(w) <= limit {
				g.lengthMasks[limit].add(i)
			}
		}
	}
	for _, w := range strings.Fields(tripleHeads) {
		g.tripleHeads.add(wordIndex[w])
	}
	for _, w := range strings.Fields(tripleAdjectives) {
		g.tripleAdjectives.add(wordIndex[w])
	}
	for a := range words {
		for b := range words {
			if a == b || blocked[[2]int{a, b}] {
				continue
			}
			g.compatible[a].add(b)
			if g.roles[a].adjective&g.roles[b].noun != 0 {
				g.adjectiveHeads[a].add(b)
			}
			if g.roles[a].modifier&g.roles[b].noun != 0 {
				g.modifierHeads[a].add(b)
			}
		}
	}
	for adjective, heads := range specificAdjectiveHeads {
		a := wordIndex[adjective]
		g.roles[a].adjective = allFamilies
		for _, head := range strings.Fields(heads) {
			g.adjectiveHeads[a].add(wordIndex[head])
		}
	}
	var pairEnd, tripleEnd uint64
	for a := range words {
		heads := g.heads(a, -1)
		if n := heads.count(); n > 0 {
			pairEnd += n
			g.pairs = append(g.pairs, phraseBlock{first: uint16(a), heads: heads, end: pairEnd})
		}
		if g.roles[a].adjective == 0 {
			continue
		}
		for b := range words {
			heads := g.heads(a, b)
			if n := heads.count(); n > 0 {
				tripleEnd += n
				g.triples = append(g.triples, phraseBlock{first: uint16(a), second: uint16(b), heads: heads, end: tripleEnd})
			}
		}
	}
	return g
}

// heads is the sole admissibility rule used for indexing and validation. A
// union of patterns counts dual-role names once, including triple overlaps.
func (g *phraseGrammar) heads(a, b int) wordSet {
	var out wordSet
	if b < 0 {
		for i := range out {
			out[i] = (g.adjectiveHeads[a][i] | g.modifierHeads[a][i]) & g.compatible[a][i]
		}
	} else {
		if g.roles[a].adjective == 0 || !g.compatible[a].has(b) {
			return out
		}
		qualified := g.qualifiers[[2]int{a, b}]
		for i := range out {
			if g.tripleAdjectives.has(a) {
				out[i] = g.adjectiveHeads[a][i] & g.modifierHeads[b][i]
			}
			if qualified {
				out[i] |= g.adjectiveHeads[a][i] & g.adjectiveHeads[b][i]
			}
			out[i] &= g.compatible[a][i] & g.compatible[b][i] & g.tripleHeads[i]
		}
	}
	// Length masks make this operation proportional to the bounded word bitset,
	// never to the Cartesian number of triples.
	prefix := len(words[a]) + 1
	if b >= 0 {
		prefix += len(words[b]) + 1
	}
	remaining := MaxPhraseBytes - prefix
	if remaining < 0 {
		return wordSet{}
	}
	for i := range out {
		out[i] &= g.lengthMasks[remaining][i]
	}
	return out
}
func (g *phraseGrammar) admits(parts []string) bool {
	var ids [3]int
	for i, p := range parts {
		id, ok := wordIndex[p]
		if !ok {
			return false
		}
		ids[i] = id
	}
	second := -1
	if len(parts) == 3 {
		second = ids[1]
	}
	return g.heads(ids[0], second).has(ids[len(parts)-1])
}
func phraseCapacity(count int) uint64 {
	var blocks []phraseBlock
	if count == 2 {
		blocks = phrases.pairs
	} else if count == 3 {
		blocks = phrases.triples
	}
	if len(blocks) == 0 {
		return 0
	}
	return blocks[len(blocks)-1].end
}
func phraseAt(index uint64, count int) (Name, error) {
	capacity := phraseCapacity(count)
	if capacity == 0 {
		return Name{}, ErrWordCount
	}
	if index >= capacity {
		return Name{}, ErrIndex
	}
	blocks := phrases.pairs
	if count == 3 {
		blocks = phrases.triples
	}
	i := sort.Search(len(blocks), func(i int) bool { return blocks[i].end > index })
	prior := uint64(0)
	if i > 0 {
		prior = blocks[i-1].end
	}
	block := blocks[i]
	head := block.heads.at(index - prior)
	text := words[block.first] + "-"
	if count == 3 {
		text += words[block.second] + "-"
	}
	text += words[head]
	return Name{Text: text, GrammarName: GrammarName, WordCount: count}, nil
}
