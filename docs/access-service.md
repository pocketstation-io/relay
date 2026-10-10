# Relay Session access service

Relay owns Session declarations, capability scope, readable join names, grant
creation and redemption, renewal, deletion, and observation fencing. The public
`access` Go package contains those rules once. A standalone `relay-server`
mounts it beside media transport. A managed control plane mounts the same module
and injects its storage adapter; the separate media Relay calls that logical
Relay access service over authenticated internal HTTP. A deployment origin is
not a second policy owner.

## One workflow

Create a Session, publish its declared AudioBuses, obtain a per-bus share link,
and open it in the receiver. Readable words only locate the grant. Both
`/join#join=<opaque-code>` and `/<words>#join=<opaque-code>` carry the same
one-use delegated credential. Neither word count nor the deprecated
`visibility` field changes authorization. The receiver sends the code in the
request body, then receives the existing Session/subscriber capability.

New readable names use the reviewed `natural-phrases-1` grammar: adjective +
noun or noun + noun; three-word names use sensory adjective + compatible noun
compound, or one of 19 reviewed adjective pairs + an eligible subject. The
frozen 1,023-word vocabulary remains readable for older links. Editorial roles
admit 148,194 pairs and 720,927 triples, with at most 24 characters in new names.
These are exact eligible namespace sizes, not service scale or security strength.
See [the naming decision](decisions/semantic-readable-names.md) for the role
subsets, compatibility rules and deliberate limits of automated curation.

Set `POCKETSTATION_NAME_POLICY=auto` (default), or any integer from `2` through
`15`. Fixed counts never change length. The library uses
`access.Config.NamePolicy`, with `access.FixedNamePolicy(n)` for a fixed count.
Auto is the default two/three-word policy: it tries short names first, switching to three
words after eight verified short-name collisions; all modes stop within 32
attempts. A storage or randomness failure never triggers fallback. Names are
reserved inside the same atomic transaction as the opaque join grant.

`POST /v1/sessions/{id}/invitations` accepts an optional integer `word_count`
with any value from 2 through 15. Omit it to use the configured policy. Null, strings, fractions,
and other values are rejected. Deprecated `visibility: public` and `private`
select two and three words respectively; do not supply both selectors. Responses
include the allocated `word_count` and the compatibility `visibility` field.
Neither field changes access rights. Both link forms still require the same
opaque join code.

Fixed counts 4–15 use `composed-phrases-1`: a sequence of reviewed short imagery
phrases, rather than an adjective stack or a claim to be a natural sentence.
Even counts use two-word groups; odd counts begin with a three-word group.
Disjoint curated palettes prevent repeats and reviewed confusables across groups.
The full name stays within 134 ASCII bytes. Two/three-word generation and its
ordering are unchanged. Exact capacities use `names.CapacityBig`; rank lookup
uses `names.AtBig`. `CapacityUint64` reports overflow explicitly, while the
deprecated `Capacity` returns zero on overflow as documented. No capacity is
silently truncated or saturated. These sizes do not strengthen authorization.

New responses always include `word_count`. Deprecated `visibility` is `public`
for two words and `private` for three or more; consumers must use the explicit
count instead of inferring three from that compatibility label.

Stored `natural-words-1` and `en-scene-v1` links retain their recorded grammar.
Grammar metadata stays inside storage; links never contain a version prefix.
The service validates stored names against that version without renaming them.

`POST /v1/sessions/{id}/renew` accepts the still-valid owner Bearer capability
and returns `source_token` and `expires_at`. It preserves Session ID, declared
buses, issuer and Session-service incarnation. An expired owner cannot renew.
Clients must renew before expiration; creating a Session does not imply an
unlimited owner credential. The owner lifetime is the smaller of the configured
owner maximum (15 minutes by default) and Session lifetime. The returned
`expires_at` is the actual signed token expiry, so renewing at half its remaining
lifetime also keeps an idle Session alive. For example,
`POCKETSTATION_SESSION_TTL_SECONDS=300` limits owner tokens to five minutes;
renew before that deadline even when no AudioBus is active. Successful renewal
extends the Session lifetime, but cannot revive an already expired Session.
Media-only publisher and subscriber token lifetimes remain independently
configured; the owner lifetime limit does not change their permissions or expiry.

## Persistence adapters

`access/storage.Store` and `Tx` are public types. Implementations supply an
atomic, serialized transaction over one configured namespace and a consistent
read snapshot. The callback runs exactly once: adapters must not retry it.
An error, cancellation or panic rolls back. A nil return acknowledges commit.
All records, transaction methods and limits are exported; no external adapter
needs a Relay internal package.

The shipped `storage/memory` adapter is process-local and loses Sessions on
restart. `storage/sqlite` retains records in one owner-readable local file,
using WAL, full synchronization and foreign keys. SQLite requires a filesystem
with its normal locking guarantees; a shared network file is not a multi-host
failover database. The managed control plane also supplies PostgreSQL through
this same port. PostgreSQL is optional.

Set `POCKETSTATION_STORAGE=memory` (default) or `sqlite` with
`POCKETSTATION_SQLITE_PATH`. The managed composition may select its PostgreSQL
adapter. A custom application can call `access.NewService(key, adapter, config)`
and mount `Register` plus authenticated `RegisterInternal` routes. This
in-process transaction interface is not a remote store gateway protocol. A
future independent persistence gateway would need explicit network failure,
transaction and authentication semantics; it is not implemented here.

The library and managed-composition defaults are 1,000 Sessions, 4,000 active
grants, 4,000 retained retry receipts, and 16 event subscribers per Session.
The shipped standalone binary defaults to 100 Sessions, 400 grants and 400
receipts; media defaults to 50 subscribers per Session. Its join admission
limit defaults to 120 requests per minute per client address and can be set
with `POCKETSTATION_INVITATION_RESOLVE_RATE_PER_MINUTE`. A serialized record is limited
to 262,144 bytes. Operations have a default five-second deadline.
Namespace serialization is a correctness-first baseline, not an unmeasured
large-scale throughput claim. The memory adapter clones capacity-limited maps per write.

## Retry and recovery

Join redemption optionally accepts `Idempotency-Key`. It commits consumption
and the full response in the same transaction. A retry needs the original
opaque code, the same locator and the same operation key. The key alone never
authenticates. The exact result is retained for at most two minutes, limited
also by grant, Session and capability expiry. Retrying does not mint a new token
or extend expiry. Receipt capacity exhaustion fails without consuming a grant.
Deleting the Session removes its grants and receipts.

`POCKETSTATION_WRITER_ID` identifies a logical access-service writer group.
Replicas in that group share serialized Session state. A different group can acquire
the next ordered term only after the previous lease expires. A displaced group
cannot pass readiness or write. This is not per-process fencing between replicas
that deliberately share a writer ID.

The media process separately acquires an ordered per-Session placement term
with its process epoch. Snapshots must match both. A delayed old process cannot
replace a newer process's observation. Observation timestamps use the storage
transaction clock, not an untrusted caller clock. Repeated snapshots refresh
liveness without false change events; expiration clears presence and advances
the authoritative revision. A later authenticated current-writer heartbeat may
restore it. No access RPC is added to packet forwarding.

Opening the service clears stored live observations. In a shared writer group,
starting another replica may therefore briefly show inactive sources until the
next media heartbeat. Durable credentials and retry receipts remain. Historical
stored readiness is never taken as proof of a currently connected device.

An older database backup cannot be distinguished automatically from legitimate
current storage without an external trust anchor. Before intentionally restoring
an older backup, stop writers and call `access.ResetRestoredStore` against the
restored adapter; this invalidates all restored Sessions, grants and receipts
and rotates the namespace incarnation. It is destructive and requires an
explicit operator recovery decision. Do not claim seamless credential-safe
rollback recovery from merely reopening a backup.

New Sessions pin the `pocketstation-relay` issuer. Managed migration may preserve
an explicitly marked legacy Session issuer and pre-migration token cutoff.
Media admission fetches that trusted profile and verifies exactly it. There is
no caller-selected issuer fallback or independent standalone subscriber policy.

## Verification and remaining evidence

Component tests cover both built-in adapters, rollback, cancellation, SQLite
reopen and schema rejection, concurrent one-use consumption, retry binding and
capacity, restart recovery, writer fencing, capability renewal, and authenticated
HTTP flows. Physical capture/browser/recording and multi-process durability
claims require separately pinned PocketStation Lab artifacts. Fault-injection
and synthetic snapshots are test fixtures, not substitute product proof.
