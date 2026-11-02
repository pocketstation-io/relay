package names

import (
	"math/big"
	"strings"
)

// ComposedGrammarName identifies several short imagery groups, not a sentence.
// Existing two/three-word names retain GrammarName and their exact enumeration.
const ComposedGrammarName = "composed-phrases-1"
const MinWordCount = 2
const MaxWordCount = 15
const MaxNameBytes = MaxWordCount*8 + MaxWordCount - 1

type phrasePalette struct{ modifiers, subjects string }

// Palettes are intentionally disjoint across groups, including the reviewed
// confusable/derivative families. Each local phrase still passes the existing
// short-phrase admissibility rule. The ordering makes boundaries deterministic:
// odd lengths start with one three-word group, then all groups have two words.
var composedPalettes = []phrasePalette{
	{`quiet soft bright calm blue green warm gentle lunar amber rice lemon velvet silver cedar maple willow pine oak birch aspen fir elm acorn`,
		`arch bay bell bloom boat book breeze bridge brook cabin canyon cape castle cave chime choir cliff cloud coast comet corpus cove crane creek dawn delta dove dream drum dune earth echo ember fern field finch fjord flute forest fox garden glade glen globe grove gull harbor haven heron hill hollow island isle lake lark leaf lily lotus meadow moon mountain nest ocean orbit owl palm path peak poem pond pool quill rain raven reef ridge river robin rose shore song spring star stone stream summit swan tale trail tulip vale valley verse vine vista wave wood wren`},
	{`clear cool deep faint light little mellow mild muted pale plain round serene silent small smooth steady still wide golden coral shell silk linen`,
		`alley arcade avenue barn basin bath bayou bend bluff camp cavern chalet chapel city corner court deck dell depot dock dome edge estate farm fence ferry floor fort forum foyer garage gate hall hamlet hangar heath hedge hotel house inlet jetty knoll lagoon lane ledge loch lodge loft market marsh mesa mill moor mound museum oasis office palace park pass patio pier plaza porch port ranch range ravine rink road roof room route school seat shelf shop stage store street studio town tunnel vault view villa village wharf window yard zoo`},
	{`fresh crisp dusky even hidden navy noble ochre open pastel polar royal rustic sandy sheer sleek snowy solar sunny tidal vivid windy azure ivory`,
		`anchor angle apron arrow atlas awning basket baton beacon bead belt bench bike blanket block board bottle bowl box broom brush bucket buckle button cable candle canoe canvas card cargo carpet chair chart clef clock coat coil comb cone cradle craft crate cube cymbal desk dial diary dish door dot drawer dress duvet easel figure film flag flask float folder font form frame fresco fringe game gift glove glyph gong grid guitar guide hammer handle`},
	{`elegant lively lucid neat rare simple sweet tawny tender tidy ample broad cozy firm grand large snug agate alloy basalt brass bronze chalk clay`,
		`accent alto anthem aria art ballad ballet band banjo bard bass beat bliss blues bongo bugle canto carol chant charm chord chorus code color comic dance debut design drama duet epic essay fable fancy finale fugue gala genre groove hum humor hymn idea jazz jingle lesson lyric magic melody motif movie muse music opera pace phrase pitch poetry polka prose`},
	{`copper cork cotton felt flannel flint glass granite jade jasper lace leather marble metal mica onyx opal paper pearl quartz ruby sable satin slate`,
		`badger beaver bison bobcat bunny camel cat chick cobra cougar cricket crow deer dog donkey duck eagle egret elk emu falcon fawn ferret fish frog gecko goat goose hare hawk horse hound jaguar junco kiwi koala lamb lemur lion llama lynx macaw magpie mink mole moose moth mouse newt orca osprey otter panda parrot penguin pet pigeon pony puma quail rabbit rhino salmon seal sheep sloth snail stork tern tiger toad trout turtle whale wolf zebra`},
	{`steel tin topaz wax wool yarn almond anise apple apricot barley basil berry cacao cherry cider cocoa coffee cumin fig ginger grape guava hazel`,
		`harp hexagon hinge hook hoop icon image jar jug kettle key kit lamp latch lens letter lid lift locket loom loop magnet mallet map mask mat mirror mitten model mosaic mural napkin needle net note number oar oven paddle page paint pan panel parade pencil photo piano piece pillow pin plank plate pocket poster pot pouch prism puzzle quilt rack radio ribbon ring robe rope rug ruler saddle sail saucer`},
	{`honey lime mango melon olive orange peach pear pepper plum saffron sesame sugar vanilla autumn dew drizzle fog frost gale hail haze ice mist`,
		`scarf score screen script sheet shoe shovel sieve skate sketch sleeve slide soap snare spool spoon stamp stitch strap string switch symbol table teapot tent thread tile towel tower train tray trunk tube vase vector veil vessel vest viola violin wagon weave wedge wheel whisk wing wire word wrap zipper pulse rhyme rhythm riff rondo rumba solo sonata sonnet stanza story style syntax tango tempo tenor theme tone trio tune visual voice volume waltz`},
}

type composedGroup struct {
	pairs, triples         []string
	pairRanks, tripleRanks map[string]int
}

var composedGroups = buildComposedGroups()

func buildComposedGroups() []composedGroup {
	groups := make([]composedGroup, len(composedPalettes))
	for i, palette := range composedPalettes {
		group := &groups[i]
		group.pairRanks = map[string]int{}
		group.tripleRanks = map[string]int{}
		modifiers, subjects := strings.Fields(palette.modifiers), strings.Fields(palette.subjects)
		for _, a := range modifiers {
			for _, b := range subjects {
				parts := []string{a, b}
				if phrases.admits(parts) {
					text := a + "-" + b
					group.pairRanks[text] = len(group.pairs)
					group.pairs = append(group.pairs, text)
				}
			}
		}
		if i == 0 {
			for _, a := range modifiers {
				for _, b := range modifiers {
					for _, c := range subjects {
						parts := []string{a, b, c}
						if phrases.admits(parts) {
							text := a + "-" + b + "-" + c
							group.tripleRanks[text] = len(group.triples)
							group.triples = append(group.triples, text)
						}
					}
				}
			}
		}
	}
	return groups
}

// CapacityBig returns a new exact integer, never a shared mutable value. Invalid
// counts have capacity zero. Counts describe names, not credential strength.
func CapacityBig(count int) *big.Int {
	if count < MinWordCount || count > MaxWordCount {
		return new(big.Int)
	}
	if count <= 3 {
		return new(big.Int).SetUint64(phraseCapacity(count))
	}
	capacity := big.NewInt(1)
	for i := 0; i < count/2; i++ {
		n := len(composedGroups[i].pairs)
		if i == 0 && count%2 == 1 {
			n = len(composedGroups[i].triples)
		}
		capacity.Mul(capacity, big.NewInt(int64(n)))
	}
	return capacity
}

// CapacityUint64 reports overflow explicitly. Use CapacityBig for all counts.
func CapacityUint64(count int) (uint64, error) {
	if count < MinWordCount || count > MaxWordCount {
		return 0, ErrWordCount
	}
	n := CapacityBig(count)
	if !n.IsUint64() {
		return 0, ErrCapacityOverflow
	}
	return n.Uint64(), nil
}

// AtBig selects an exact rank without expanding the Cartesian group product.
// The caller's index is never mutated. Negative and out-of-range ranks fail.
func AtBig(index *big.Int, count int) (Name, error) {
	capacity := CapacityBig(count)
	if capacity.Sign() == 0 {
		return Name{}, ErrWordCount
	}
	if index == nil || index.Sign() < 0 || index.Cmp(capacity) >= 0 {
		return Name{}, ErrIndex
	}
	if count <= 3 {
		return phraseAt(index.Uint64(), count)
	}
	rank := new(big.Int).Set(index)
	texts := make([]string, count/2)
	for i := len(texts) - 1; i >= 0; i-- {
		pool := composedGroups[i].pairs
		if i == 0 && count%2 == 1 {
			pool = composedGroups[i].triples
		}
		remainder := new(big.Int)
		rank.QuoRem(rank, big.NewInt(int64(len(pool))), remainder)
		texts[i] = pool[remainder.Int64()]
	}
	return Name{Text: strings.Join(texts, "-"), GrammarName: ComposedGrammarName, WordCount: count}, nil
}
func parseComposed(parts []string) bool {
	if len(parts) < 4 || len(parts) > MaxWordCount {
		return false
	}
	offset := 0
	for i := 0; i < len(parts)/2; i++ {
		width := 2
		lookup := composedGroups[i].pairRanks
		if i == 0 && len(parts)%2 == 1 {
			width = 3
			lookup = composedGroups[i].tripleRanks
		}
		if _, ok := lookup[strings.Join(parts[offset:offset+width], "-")]; !ok {
			return false
		}
		offset += width
	}
	return offset == len(parts)
}
