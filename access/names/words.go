package names

import "strings"

// One curated pool may appear in any position. These are ordinary words, not
// fabricated compounds or padding. Most are 3–6 letters. The explicitly reviewed
// 7–8 letter exceptions are checked in names_test.go. The original 285 words
// remain available; stored names are concrete text, never enumeration indices.
var words = strings.Fields(`
acorn agate alder aloe amber anise apple arch aspen aster azure bamboo
bank basalt basin bay beach beech bend berry birch bloom blue bluff
bold bough briar bridge bright brisk brook bud cacao calm canyon cape
cavern cedar cherry chime cinder circle clay clean clear cliff clover coast
comet cool coral corpus cosmos court crane creek crest crisp daisy dawn
deep dell delta dove dune dusky eagle elm ember even faint fair
falcon falls fawn field finch fine fir firm fjord flint flora flower
flute forest fox fresh frost garden gentle geode glad glade glass glen
golden gorge grain grass green grove gull harbor haven hazel heath heron
hidden high hill hollow holly honey inlet iris isle ivory jade jasper
junco kelp kind knoll lagoon lake lane larch lark laurel leaf ledge
lemon light lilac linen little lively loch lotus lucid lunar maple marble
marsh meadow mellow mesa mica mild mint misty moon moor mossy moth
mound mountain muted myrtle navy neat nectar nest noble oak oasis oats
ocean ochre olive onyx opal open orchid otter owl pale palm panda
park pass pastel path peak pearl pearly pebble petal pine plain plum
pond pool poppy port quartz quiet quill rain rare raven ravine reed
reef rice ridge river road robin rose rosy round royal rue rush
rustic sage sand sandy seal serene sheer shell shore silent silk silly
silver slate sleek slope slow small smooth snow snowy soft solar spring
spruce square star steady steel still stork stream summit sun sunny swan
swift tawny tender thyme tidal tiger trail true tulip vale valley velvet
vine violet vista vivid warm wharf wide wild willow windy winter wise
wisp wood wren yard yarn yarrow yew young zinnia able accent agile
air alley alloy almond alto amble ample anchor angle anthem apricot apron
arcade ardent aria aroma arrow art atlas autumn avenue awning azalea badger
bagel balance ballad ballet balm banana band banjo bard barley barn basil
basket bass bath baton bayou beacon bead bean beat beaver beet bell
belt bench bike bison blanket bliss block blues board boat bobcat bongo
bonsai book bottle bounce bowl box brass brave bread breeze brief brine
broad bronze broom brush bucket buckle bugle bulb bun bunny butter button
cabin cable cactus camel camp candid candle canoe canto canvas card care
cargo carol carpet carrot carry cashew castle cat cave celery cello chair
chalet chalk chant chapel charm chart cheese chick chili chive choice choir
chord chorus cider citrus city clef clever click climb clock cloth cloud
clove clue coat cobra cocoa code coffee coil color comb comic cone
cookie copper cork corn corner cotton cougar cove cozy cradle craft crate
cream crepe cricket crocus crow cruise cube cumin cup curry curve cycle
cymbal daffodil dahlia dance dash date debut deck deer depot depth design
desk dew dial diary dish dock dog dome donkey door dot dough
drama draw drawer dream dress drift drizzle drum duck duet dust duvet
eager early earth ease easel easy echo edge egg egret elegant elk
emu epic essay estate fable fall fancy farm feast felt fence fennel
fern ferret ferry fiddle fig figure film finale fish flag flannel flask
flax float floor flour flow fog foil folder font form fort forum
foyer frame fresco fringe frog frond fruit fudge fugue future gala gale
game garage garlic garnet gate gecko gem genre gift ginger glide globe
glove glyph goat gold gong goose grace grand granite grape gravel gravy
grid groove guava guide guitar gust hail hall halo hamlet hammer handle
hangar happy hare harp hawk haze heart hedge herb hexagon hike hinge
hook hoop hope horse hotel hound house hover hum humor hymn ice
icing icon idea ideal image indigo ink island ivy jaguar jar jasmine
jazz jelly jetty jingle jog jug juice jump kale kettle key kit
kiwi koala lace lamb lamp large latch laugh leap leather legacy lemur
lens lentil lesson letter lichen lid lift lily lime line lion llama
loaf locket lodge loft logic loom loop luck lynx lyric macaw magic
magnet magpie mallet mango map march market mask mat melody melon merry
metal milk mill millet mink mirror mist mitten mocha model mole moose
mosaic motif mouse move movie mural muse museum music napkin needle net
newt nimble noodle note number nut oar office onion opera orange orbit
orca osprey oval oven pace paddle page paint pair palace pan panel
papaya paper parade parrot patio peace peach peanut pear pecan pedal pencil
penguin peony pepper pet photo phrase piano pickle piece pier pigeon pillow
pin pita pitch pivot pizza plank plate plaza plush pocket poem poetry
poise polar polka pollen pony porch poster pot pouch prime prism propel
prose prune pulp pulse puma puzzle quail quilt quince rabbit race rack
radio radish raisin ramen ranch range ready recipe relish rhino rhyme rhythm
ribbon ride riff ring rink ripe rise roam roast robe rock roll
rondo roof room root rope route ruby rug ruler rumba sable saddle
saffron sail salad salmon salsa satin sauce saucer scarf school scoot score
screen script seat seed sesame shape sheep sheet shelf shift shoe shoot
shop shovel shower sieve simple skate sketch skill sleet sleeve slide sloth
smile snail snare snug soap soar solo sonata song sonnet soup space
spark sphere spice spin spiral spirit spool spoon squash stage stamp stanza
starch steer stem step stew stitch stone store storm story strap straw
street stride string stroll studio style sugar summer sunset sushi sway sweep
sweet swim swing switch symbol syntax syrup table taco tale tango tap
teapot tempo tenor tent tern thaw theme thorn thread thunder tidy tile
tilt timber tin toad toast toffee tomato tone topaz towel tower town
train travel tray trek trifle trio trout trunk trust tube tumble tune
tunnel turn turnip turtle twist unity vanilla vapor vase vault vector veil
verse vessel vest view villa village vinyl viola violin visual voice volume
waffle wagon wake walk wall walnut waltz wander water wave wax weave
wedge whale wheat wheel whim whirl whisk white window wing wire witty
wolf wonder wool word wrap yeast yogurt zeal zebra zero zest zip
zipper zoo zoom
`)
