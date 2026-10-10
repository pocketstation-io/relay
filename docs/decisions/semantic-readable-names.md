# Semantic readable names

Status: accepted for new allocations. Existing stored links retain their grammar.

## Purpose

A share link should be easy to say, distinguish and remember. Readable words
locate a grant; only the opaque join code authorizes its redemption. Word count,
word choice and namespace size never add permission or confidentiality.

The former `natural-words-1` grammar permuted any distinct words from one pool.
That made arithmetic capacity large but produced awkward word order. New
`natural-phrases-1` allocations use explicit editorial roles and compatibility.
A naming grammar belongs to Relay's access service in both deployment modes.

## Reviewed vocabulary and patterns

The frozen pool contains 1,023 familiar words, mostly three to six letters,
with 18 retained seven/eight-letter exceptions. The source lexicon assigns 910
subject nouns, 102 adjectives and 161 noun modifiers; roles can overlap. These
are explicit curated lists, not runtime dictionary guesses.

Twenty words are retained only for old links: falls, mossy, rush, slow, square,
true, able, ardent, carry, cello, click, oval, plush, prime, propel, ripe, roam,
scoot, shower and steer. Their absence from generation is an editorial scope
choice, not a claim that they can never be English nouns or adjectives. No word
was removed from legacy parsing to inflate or disguise the new quality rules.

Two-word forms are adjective + noun and noun + noun. Subject families cover
nature, places, animals, plants, food, materials, objects, music and ideas.
Adjectives and noun modifiers have reviewed target-family masks. Mental or
animate descriptors have narrower subjects; for example, agile-rink and
easy-mound are excluded. Required examples remain eligible: quiet-orbit,
rice-river, silly-mountain, lemon-corpus and velvet-moon.

Three-word forms are independently constrained:

- A reviewed sensory adjective + compatible noun modifier + a reviewed imagery
  subject. The subject list emphasizes landmarks, natural objects and music.
  Examples include quiet-rice-river and soft-velvet-moon.
- One of 19 ordered adjective pairs + an eligible imagery subject, compatible
  with both adjectives. Examples include pale-blue-moon and warm-golden-dove.
  Arbitrary adjective permutations are excluded.

The same predicate excludes repeated words, reviewed confusable combinations
(such as beach/beech and rice/rise), derivatives (such as pear/pearly and gold/golden), specific unwanted associations, and names
longer than 24 characters including hyphens. Color-only indigo, navy and ochre
are excluded as noun modifiers; stacked color subjects such as
pastel-ochre-haven are not generated. High, wide, steady and sheer are not first
adjectives in the noun-compound triple form. Compatibility lists are small,
explicit and testable; they do not promise universal phonetic disambiguation,
universal cultural safety, or that every metaphor is a literal English phrase.

The exact admitted capacity is **148,194 pairs and 720,927 triples**. The union
of applicable patterns counts each textual name once even when roles overlap.
These counts measure eligible names, not throughput, concurrent Session capacity,
entropy of a credential or a billion-user scaling claim.

## Finite indexing and allocation

Generation selects a uniform random rank with cryptographic rejection sampling,
then resolves it through prefix counts and bitsets. The same admissibility
predicate builds the index and validates current names. No random retry loop
searches for a linguistically valid phrase, and triples are not materialized.
Compatibility/length bitsets keep initialization work proportional to the vocabulary
and eligible prefixes rather than a Cartesian triple scan.

The measured development build initializes the grammar in about 30–45 milliseconds,
allocating about 6.2 MB temporarily and retaining roughly 1.44 MB of prefix-index
capacity, plus role/compatibility tables. Those measurements are local component
observations, not a portable latency guarantee. Tests enumerate every eligible
pair, inspect triple block boundaries and strata, verify recorded grammars,
validate all editorial references and retain deterministic sample output.

The service defaults to `auto`: start with two words, switch to three after
eight verified occupied short names, and stop after 32 total attempts. Fixed
`2` and `3` policies also have 32 total attempts. Code/record conflicts consume
the same finite budget but do not imply pressure in the short-name namespace.
Storage or randomness errors return immediately. Allocation failure never
replaces an existing grant or silently changes permissions.

Applications set `access.Config.NamePolicy`; standalone and managed binaries
share `POCKETSTATION_NAME_POLICY=auto|2|3`. An individual creation request may
specify integer `word_count: 2` or `3`. Omission selects the configured policy;
null, strings, fractions and other values are invalid. Deprecated explicit
`visibility` still selects its old two/three-word format. Supplying both
selectors is rejected even if their values would agree. The returned word count
reports the format actually allocated.

## Compatibility and boundaries

`ParseStored` verifies a name against its stored grammar. `natural-words-1`
retains the previous distinct-word pool semantics; `en-scene-v1` retains the
original compound parser. Existing links are neither regenerated nor renamed.
New generation never uses those old grammars. Normal parsing may recognize old
text, but that never proves a stored grant exists or authorizes redemption.

The access service reserves names atomically through its public storage port.
Clients choose presentation preferences and consume returned links; they do not
copy grammatical or authorization rules. No grammar version appears in a URL.
No new credential category, live mock, or alternate security interface is added.


## Extension: developer-selected lengths 2 through 15

The default remains the two/three-word collision policy above. A developer may
instead configure any fixed integer from 2 through 15. That fixed count never
grows or shrinks when names collide. The same 32-attempt reservation budget
and opaque-code security rules apply. Explicit deprecated visibility selects
only its old two/three-word format; it cannot accompany an explicit word count.

Two- and three-word grammar, ranks and outputs are unchanged. Counts 4–15 use
`composed-phrases-1`, a sequence of imagery phrases. Even counts concatenate
reviewed two-word groups. Odd counts begin with a reviewed three-word group,
followed by two-word groups. There are at most seven groups. For example:
`fir-grove-golden-bend` is read as fir-grove / golden-bend, while a longer
example starts quiet-pine-ocean / small-mesa / even-beacon. These are memorable
phrase groups, not a promise that a fifteen-word identifier is an English
sentence. Default links remain short.

Seven explicit palettes partition modifiers and subjects. Every local pair or
triple passes the existing semantic admissibility predicate. The palettes are
disjoint across groups, including the reviewed confusable and derivative groups,
so full names cannot repeat words or combine those conflicting variants. Groups
use natural imagery, places, objects, music and animals. Source lists are
editorial data; generation does not infer English roles at runtime. The finite
local group lists are materialized once, never the exponential product of full
names. Mixed-radix indexing selects each full name exactly once.

Exact capacities are:

| Words | Admitted names |
| --- | ---: |
| 2 | 148194 |
| 3 | 720927 |
| 4 | 4394208 |
| 5 | 27955200 |
| 6 | 7909574400 |
| 7 | 50319360000 |
| 8 | 11389787136000 |
| 9 | 72459878400000 |
| 10 | 20774971736064000 |
| 11 | 132166818201600000 |
| 12 | 34901952516587520000 |
| 13 | 222040254578688000000 |
| 14 | 61985867669459435520000 |
| 15 | 394343492131749888000000 |

`CapacityBig` returns a fresh exact big integer; callers cannot mutate shared
state. `AtBig` does not mutate its input rank. Cryptographic rejection sampling
selects uniformly across the exact capacity. `CapacityUint64` reports overflow;
the deprecated `Capacity` returns zero for invalid counts or overflow, never a
wrapped or saturated value. Production generation uses the exact APIs. Twelve
through fifteen words exceed uint64 with the current palettes. These are
combinatorial sizes, not measured deployment scale or access strength.

New composed names are limited to 134 ASCII bytes (15 × 8 + 14). Generic locator
parsing allows 2–15 lowercase alphabetic words of 3–24 letters subject to the
same total bound, preserving older compound links. Stored grammar validation is
stricter: the old grammars still accept only their original two/three-word forms,
and composed names must match their positional phrase groups. Store locator
columns remain text, and record/HTTP limits already exceed this finite bound.
No persistence schema migration or rewriting of existing links is required.

New creation and inspection responses carry the actual integer `word_count`.
The deprecated visibility label is public for two and private for all higher
counts; it does not encode the count or permissions. Old responses without the
count may be interpreted only using the legacy two/three-word rules. API input
rejects null, booleans, fractions, strings, values outside 2–15 and simultaneous
visibility plus word_count. Canonical configuration strings reject padded or
fractional numeric forms as well.
