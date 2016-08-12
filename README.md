# NEAT experiments

This repo is a test-bed for experiments with NEAT.

The main idea behind NEAT is to split up full history OSM data into tiles, and
present that in such a way that other layers of processing can be built on top
of it.

## Key goals

Key goals for NEAT:

1. Fast updates
2. Changes as "diffs"

### Fast updates

OSM updates every minute, and NEAT should be able to update with a similar
cadence.

### Changes as "diffs"

Updates to OSM are exposed as a series of changes encapsulated in a linear
sequence of files. This means other processing layers can "follow along" with
OSM's changes efficiently without needing to re-download the whole state just to
check if it has changed. NEAT's updates should be accessible in a similar way,
so that later processing layers can follow just the changes and not re-download
the whole file(s).

## Versions of the experiment

### Neatlacoche

[Neatlacoche](https://github.com/tilezen/neatlacoche) was the first version,
which was a "single-shot" implementation to take the planet history file and
split it up into small tiles. The memory overhead required to do that was
enormous, and it was very slow. There didn't seem to be much of a way to get
diffs working with that, so it was abandoned.

### NEAT yet again

NEAT yet again, in the `neat_yet_again` subdirectory, was a Python test bed to
try and figure out the mechanics of doing streaming joins.

#### Implementation

Although this is just a test-bed, and may or may not directly evolve into
"production" NEAT code, it is intended as a way to scope out the data structures
and implementation methods needed to realise NEAT.

The first layer of code was around the `Relation` class, which describes the
transformations required to produce a NEAT tile in declarative relational
algebra, much like SQL. Although this relational model uses a set model rather
than SQL's bag model. This has pros and cons; it is more difficult to implement,
but does not require reasoning about steps for explicit duplication removal.

The second layer was around the `Stream` class, which takes the relational
algebra and implements it in a streaming manner, with updates being pushed into
the network of `Stream`s and the `Stream` outputting any new elements. The data
model for OSM's history is append-only, and therefore it isn't necessary at this
point to model deletions. This gives some flexibility in allowing relaxed time
ordering.

The third layer was around externalising the data structures used in the
`Stream` so that they are available for reuse by later layers in the stack.

#### Externalised indexes

To process tuples in parallel, we can:

1. Shard the tuples by key. Jobs are issued for a partition of the key space,
which effectively means the job execution system is a locking service, with
all the problems that brings.
2. Regardless of any sharding, make the externalised index data structure safe
for concurrent access. For example, modelling it as a CRDT.

#### CRDT

* Trouble with a CRDT is that writes can be "lost" or arbitrarily delayed. But
on the positive side, means that jobs can be retried as many times as
necessary. Implies deterministic output naming?
* Can control ordering with epochs, but can it be done in a way that doesn't
limit the degree of parallelism?

### Current version

This is yet another version, aiming to implement the streaming part of the
NEAT algorithm, but only on a single machine. Extending to distributed work
adds a lot of complexity which makes it harder to implement the core
algorithm correctly.

The core idea is to maintain all the data needed for the streaming joins in
[LevelDB](https://github.com/google/leveldb), and emit pairs of
`(element, tile)` to a data structure called a buffered repository trie, which
is analogous to the
[buffered repository tree](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.27.9904),
but with a fixed layout. Each node in the quadtree (trie) has a buffer and only
writes down to its children when the buffer overflows. This keeps the number of
writes minimal and improves cacheability of the leaves.

The algorithm for this is roughly:

1. Start with the current node as the root.
2. If the current node's buffer is not full, then insert `(element, tile)` into
the current node's buffer.
3. Else, for each `(element, tile)` pair in the current buffer, insert it into
the child node representing the next level up in the quadtree.

Inserts should be batched, and each batch forms a separate file on S3.
Serialising updates as `(element, list of tile)` might be more practical,
especially near the root. When overflowing the buffer and resetting it, a new
directory for the node can be allocated to avoid deleting the old buffer files.
Directory "pointers" from parents will need to be updated, but those nodes were
also flushed, so the write can be coalesced.

When querying this data structure, the nodes are enumerated from the root up
to the leaf for the tile(s) being queried, filtering intermediate buffers for
relevant entries. Because buffers are of fixed number of entries, `B`, then a
query will complete in a maximum of `B * D`, where D is the depth of the
quadtree, which is worst-case `log N / log B` GET operations, where `N` is the
number of writes to the trie.

The data structure is semi-persistent (append-only), so queries can operate
without locks and without interfering with other reads or writes. Multiple
writers would need locks, though.

To create a NEAT tile, the trie is traversed from the root to the leaf,
gathering all versions of elements in the matching tile. Once they have all
been gathered, they can be sorted into the correct order. Each batched
update could be sorted before being inserted into the child node, which
could provide partial input for a merge sort.
