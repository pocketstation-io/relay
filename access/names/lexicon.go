package names

// This is editorial data, not inferred part-of-speech tagging. The frozen
// 1,023-word vocabulary remains available to parse earlier stored links. Words
// without a role below are deliberately excluded from new phrase generation.
// Families describe naming imagery, not an exhaustive English dictionary.
type family uint16

const (
	nature family = 1 << iota
	place
	animal
	plant
	food
	material
	object
	music
	idea
	allFamilies = nature | place | animal | plant | food | material | object | music | idea
)

type lexicalGroup struct {
	family family
	words  string
}

var nounGroups = []lexicalGroup{
	{nature, `air arch autumn cinder crest stream wisp breeze circle cloud comet cosmos dawn delta dew drizzle dust earth echo ember fall fog frost gale globe gust hail halo haze hill ice island light line mist moon mountain ocean orbit rain ridge river rock sleet slope snow space spark sphere spring star stone storm summer sun sunset thunder vapor water wave winter`},
	{place, `alley arcade avenue bank barn basin bath bay bayou beach bend bluff bridge brook cabin camp canyon cape castle cave cavern chalet chapel city cliff coast corner court cove creek deck dell depot dock dome dune edge estate farm fence ferry field fjord floor forest fort forum foyer garage garden gate glade glen gorge grove hall hamlet hangar harbor haven heath hedge hollow hotel house inlet isle jetty knoll lagoon lake lane ledge loch lodge loft market marsh meadow mesa mill moor mound museum nest oasis office palace park pass path patio peak pier plain plaza pond pool porch port ranch range ravine reef rink road roof room route school seat shelf shore shop stage store street studio summit town trail tunnel vale valley vault view villa village vista wall wharf window yard zoo`},
	{animal, `badger beaver bison bobcat bunny camel cat chick cobra cougar crane cricket crow deer dog donkey dove duck eagle egret elk emu falcon fawn ferret finch fish fox frog gecko goat goose gull hare hawk heron horse hound jaguar junco kiwi koala lamb lark lemur lion llama lynx macaw magpie mink mole moose moth mouse newt orca osprey otter owl panda parrot penguin pet pigeon pony puma quail rabbit raven rhino robin salmon seal sheep sloth snail stork swan tern tiger toad trout turtle whale wolf wren zebra`},
	{plant, `acorn alder aloe aspen aster azalea bamboo beech birch bloom bonsai bough briar bud bulb cactus cedar cherry clover crocus daffodil dahlia daisy elm fennel fern fir flax flora flower frond fruit grass grove hazel holly iris ivy jasmine kelp larch laurel leaf lichen lilac lily lotus maple myrtle oak orchid palm peony petal pine pollen poppy reed root rose rue sage seed spruce stem straw thorn thyme timber tulip vine violet walnut wheat willow wood yarrow yew zinnia`},
	{food, `almond anise apple apricot aroma bagel balm banana barley basil bean beet berry brine bread bun butter cacao carrot cashew celery cheese chili chive cider citrus clove cocoa coffee cookie corn cream crepe cumin cup curry date dough egg feast fig flour fudge garlic ginger grain grape gravy guava herb honey icing jelly juice kale lemon lentil lime loaf mango melon milk millet mint mocha nectar noodle nut oats olive onion orange papaya peach peanut pear pecan pepper pickle pita pizza plum prune pulp quince radish raisin ramen recipe relish rice roast saffron salad salsa sauce sesame soup spice squash starch stew sugar sushi syrup taco toast toffee tomato trifle turnip vanilla waffle yeast yogurt zest`},
	{material, `agate alloy amber azure basalt brass bronze chalk clay cloth copper coral cork cotton felt flannel flint foil garnet gem geode glass gold granite gravel indigo ink ivory jade jasper lace leather linen marble metal mica navy ochre onyx opal paper pearl pebble quartz ruby sable sand satin shell silk silver slate steel tin topaz velvet vinyl wax white wool yarn`},
	{object, `anchor quill angle apron arrow atlas awning basket baton beacon bead bell belt bench bike blanket block board boat book bottle bowl box broom brush bucket buckle button cable candle canoe canvas card cargo carpet chair chart clef clock coat coil comb cone cradle craft crate cube cymbal desk dial diary dish door dot drawer dress drum duvet easel fiddle figure film flag flask float folder font form frame fresco fringe game gift glove glyph gong grid guitar guide hammer handle harp hexagon hinge hook hoop icon image jar jug kettle key kit lamp latch lens letter lid lift locket loom loop magnet mallet map mask mat mirror mitten model mosaic mural napkin needle net note number oar oven paddle page paint pair pan panel parade pencil photo piano piece pillow pin plank plate pocket poster pot pouch prism puzzle quilt rack radio ribbon ring robe rope rug ruler saddle sail saucer scarf score screen script sheet shoe shovel sieve skate sketch sleeve slide soap snare spool spoon stamp stitch strap string switch symbol table teapot tent thread tile towel tower train tray trunk tube vase vector veil vessel vest viola violin wagon weave wedge wheel whisk wing wire word wrap zipper`},
	{music, `accent chime flute alto anthem aria art ballad ballet band banjo bard bass beat bliss blues bongo bugle canto carol chant charm choir chord chorus code color comic corpus dance debut design drama dream duet epic essay fable fancy finale fugue gala genre groove hum humor hymn idea jazz jingle lesson lyric magic melody motif movie muse music opera pace phrase pitch poem poetry polka prose pulse rhyme rhythm riff rondo rumba solo sonata song sonnet stanza story style syntax tale tango tempo tenor theme tone trio tune verse visual voice volume waltz`},
	{idea, `amble balance bounce care choice climb clue cruise curve cycle dash depth draw drift ease flow future glide grace heart hike hope hover ideal jog jump laugh leap legacy logic luck march move peace pedal pivot poise race ride rise roll shape shift shoot skill smile soar spin spiral spirit step stride stroll sway sweep swim swing tap thaw tilt travel trek trust tumble turn twist unity wake walk wander whim whirl wonder zeal zero zip zoom`},
}

// Adjective groups are paired only with the listed subject families. Broad
// sensory descriptors are distinct from playful/animate descriptors.
var adjectiveGroups = []lexicalGroup{
	{allFamilies, `blue bright clear cool deep faint fine golden green high light little mellow mild muted pale plain quiet round serene silent small smooth soft steady still warm white wide`},
	{nature | place | material | object | music | idea, `amber azure bold calm clean crisp dusky even fresh hidden lunar misty navy noble ochre open pastel pearly polar rosy royal rustic sandy sheer sleek snowy solar sunny tidal vivid windy`},
	{nature | place | plant | food | material | object | music | idea, `brisk elegant gentle ivory lively lucid neat rare simple sweet tawny tender tidy velvet`},
	{animal, `agile brave clever eager glad happy kind nimble swift wise young`},
	{place | object | music | idea, `ample broad cozy firm grand large noble snug`},
	{nature | place | animal | music, `silly wild merry fair`},
}

// A noun modifier is curated independently of noun eligibility. Family masks
// define the intended metaphor: rice-river, velvet-moon, lemon-corpus. They do
// not admit arbitrary permutations of every subject noun.
var modifierGroups = []lexicalGroup{
	{nature | place | animal | object | music, `agate alloy amber basalt brass bronze chalk clay copper coral cork cotton felt flannel flint glass gold granite ivory jade jasper lace leather linen marble metal mica onyx opal paper pearl quartz ruby sable sand satin shell silk silver slate steel tin topaz velvet wax wool yarn`},
	{nature | place | object | music, `almond anise apple apricot barley basil berry cacao cherry cider cocoa coffee cumin fig ginger grape guava hazel honey jasmine lemon lilac lime lotus mango melon mint olive orange peach pear pepper plum rice rose saffron sage sesame sugar thyme vanilla violet walnut`},
	{place | animal | plant | object | music, `autumn breeze cloud dawn dew drizzle earth ember fog frost gale hail haze ice mist moon ocean rain river snow spring star storm summer sun sunset thunder vapor water wave winter`},
	{nature | place | object | music, `acorn alder aloe aspen aster bamboo beech birch cedar clover daisy elm fern fir flax iris ivy kelp larch laurel leaf lily maple myrtle oak orchid palm peony petal pine poppy reed spruce straw tulip vine willow wood yarrow yew`},
}

// Three-word adjective sequences are individually reviewed ordered pairs.
// These are combined with the intersection of both adjectives' subject masks.
var qualifierPairs = []string{
	"quiet little", "bright little", "soft golden", "pale blue", "deep blue",
	"clear blue", "warm golden", "soft green", "quiet green", "small golden",
	"mellow little", "gentle little", "cozy little", "crisp white",
	"pale green", "soft white", "warm little", "still blue", "wild little",
}

// Confusable words must not coexist in one generated name. Legacy stored names
// remain readable. This small reviewed list makes no universal phonetic claim.
var confusableGroups = []string{
	"pear pearly", "sun sunny", "snow snowy", "sand sandy", "rose rosy", "gold golden", "mist misty",
	"beach beech", "flour flower", "rice rise", "pear pair",
	"mist mint",
}
var disallowedPairs = []string{
	"silly goose", "wild goose",
}

// Specific heads prevent mental/temporal adjectives from leaking through broad
// family tags into phrases such as agile-rink or easy-mound.
var specificAdjectiveHeads = map[string]string{
	"candid": "essay story verse", "witty": "comic essay poem story verse",
	"brief": "echo note poem tale verse", "early": "dawn spring summer winter",
	"easy": "flow pace rhythm tune", "ready": "kit",
}

// Triples use a smaller independently reviewed imagery palette. This is a
// deliberate subset, not every noun admitted by the two-word grammar.
var tripleHeads = `arch bay beach bell bloom boat book breeze bridge brook cabin canyon cape castle cave cedar chime choir cliff cloud coast comet corpus cove crane creek dawn delta dove dream drum dune earth echo elm ember fern field finch fjord flute forest fox garden glade glen globe grove gull harbor haven heron hill hollow island isle lake lark leaf lily lotus maple meadow moon mountain nest oak ocean orbit owl palm path peak pine poem pond pool quill rain raven reef ridge river robin rose shore song spring star stone stream summit sun swan tale trail tulip vale valley verse vine vista wave willow wood wren`
var tripleAdjectives = `amber azure blue bright calm clear cool cozy crisp deep dusky faint fresh gentle golden green hidden ivory light little lively lucid lunar mellow mild misty muted navy ochre pale pastel pearly plain polar quiet rosy round royal sandy serene silent silver sleek small smooth snowy soft solar still sunny sweet tawny tender tidal velvet vivid warm white windy`
