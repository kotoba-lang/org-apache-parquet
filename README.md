# org-apache-parquet

**A Parquet reader in portable `.cljc`**, providing
[`columnar`](https://github.com/kotoba-lang/columnar)'s `IColumnSource`. No
Rust, no JNI, no native library — the format is decoded from bytes.

```clojure
(require '[parquet.source :as pq] '[columnar.plan :as plan])

(plan/scan (pq/open bytes) {:columns ["price"] :predicates [[:= "price" 120]]})
;; => {:rows [{::plan/row 4 "price" 120}] :chunks-read 1 :chunks-skipped 2}
```

Origin plane: the format is Apache's, so the repo is named for where it comes
from (`parquet.apache.org` → `org-apache-parquet`), not for what it does here.

## Statistics work on files this reader cannot decode

The single most useful property, and the reason a partial reader is worth
having. Encoding and compression are properties of the **pages**; min/max,
null counts and row counts live in the **footer**. So a snappy,
dictionary-encoded file — which `read-column` refuses — still prunes, still
answers `count`, `min` and `max`, and does it reading nothing but metadata.

There is a fixture written with snappy + dictionary specifically to hold that
line, and a test asserting both halves: the refusal names the codec, and the
aggregate returns `{:from :statistics :read 0}`.

## What it decodes, and what it refuses by name

Supported: codecs `UNCOMPRESSED`, **`SNAPPY`** and **`GZIP`**; encodings `PLAIN`,
**`PLAIN_DICTIONARY`** and **`RLE_DICTIONARY`**; v1 `DATA_PAGE`; flat schemas;
`REQUIRED` and `OPTIONAL`; `INT32` / `INT64` / `DOUBLE` / `BYTE_ARRAY`.

That set is **what pyarrow writes by default**, so most real files are now
readable rather than only prunable.

Everything else throws with what it met — zstd / brotli / lz4, delta
encodings, byte-stream-split, v2 data pages, repeated (nested) columns. **A
decoder that guesses at an encoding produces plausible numbers**, and
plausible numbers from a file nobody re-checks is the worst failure this
library could have: silent, and downstream of every guard.

### How the snappy + dictionary path is checked

`dictionary-snappy.parquet` is asserted to decode **value-for-value and
null-for-null identical to `plain.parquet`** — the same dataset written two
completely different ways, decoded independently. Without a second
implementation to diff against, cross-encoding agreement is the strongest
check available.

Snappy here is the **raw** format, not framed: Parquet compresses each page
body alone and records both lengths in the page header, so the stream framing
has no job. The preamble is checked against the header's number rather than
trusted, because framed bytes fed to a raw decoder read their frame header as
a length.

The one subtlety in snappy is that a copy element may reference bytes it is
*currently producing* — an offset smaller than the length, which is how a
repeating run is encoded. Copies are therefore byte-at-a-time from the growing
output; slicing the source up front is the natural-looking implementation and
truncates every run longer than its own period.

`delta.parquet` exists to stay unsupported. When snappy became readable, that
fixture is what kept "statistics work on files this reader cannot decode"
under test instead of quietly losing its only witness — the refusal tests had
been passing against a file that had just become decodable.

gzip is decoded through **`kotoba-lang/org-ietf-deflate`** — Huffman + LZ77 by
hand in portable `.cljc`, verifying CRC-32 and ISIZE. `java.util.zip` and
Node's `zlib` would each work on exactly one of the runtimes this reader has
to keep running on, and a second inflate in this workspace would be a second
one to get wrong. `gzip.parquet` is asserted to decode value-for-value
identical to `plain.parquet`, and is **excluded from the byte-determinism
gate** for the reason below.

The unsupported fixture is DELTA_BINARY_PACKED and not gzip, though gzip was
the obvious choice:
**gzip byte-equality is not a property that holds across environments.**
Deflate output is implementation-dependent, so the generator produced
identical bytes on repeated local runs and different bytes on CI. The
determinism gate caught it. Codec refusal is covered instead by unit tests on
`check-readable!`, which is pure — it needs no fixture and can name every
codec, where a fixture per codec would be one more binary required to be
byte-identical everywhere.

`check-readable!` runs before any page byte is fetched, so a refusal costs a
metadata lookup rather than a download.

## Integers are exact, or refused — never rounded

The obvious little-endian decode is `(+ acc (* byte (Math/pow 2 (* 8 k))))`,
and it is **wrong for int64 on both runtimes**: `2^56 × byte` is past 2^53, so
the top bytes of a 64-bit value land beyond double precision and come back
quietly rounded. On the JVM `Math/pow` returns a double, so the sum is already
inexact before anything casts it back to `long`.

So `le-uint` uses exact integer arithmetic on the JVM, and on ClojureScript —
where numbers *are* doubles — refuses a value past `MAX_SAFE_INTEGER` instead
of rounding it. There is a fixture column holding `2^62 + 1`: the JVM test
asserts the exact value, the ClojureScript test asserts the refusal.

**But a statistic that cannot be represented is dropped, not raised.** The two
cases differ in whether a safe degradation exists — an absent bound means
`columnar.stats` will not prune, which is slower and still correct, while an
unrepresentable value has no safe answer at all. Raising in the footer made a
single oversized column render the whole file unopenable on ClojureScript,
including for the columns that were fine. That was measured, not predicted.

## Fixtures come from the reference implementation

`test/fixtures/*.parquet` are written by **pyarrow**, regenerated by
`test/fixtures/generate.py`. That script is the one piece of Python here and
it earns the exception: the only trustworthy oracle for a decoder is a file
written by the format's own writer. A fixture this repo generated itself would
test the decoder against its own misunderstanding, and a decoder that agrees
with its own encoder is confidently wrong on every real file.

```sh
python3 -m venv .venv && .venv/bin/pip install pyarrow
.venv/bin/python test/fixtures/generate.py
```

## It reads ranges, not files

An earlier version of this reader took the whole file as a vector. It was
correct, and it threw away the point: Parquet exists so a query reads a footer
and two column chunks instead of forty gigabytes.

`parquet.bytes/IByteSource` answers ranges, and every read goes through it:

| operation | what is fetched |
|---|---|
| `open` | leading magic (4 B), the tail (8 B), and the footer — **no data pages, at any file size** |
| `-chunk-stats` | nothing; a lookup in what `open` already parsed |
| `-read-column` | exactly `total_compressed_size` for that one chunk, at its `data_page_offset` |
| a refused chunk | **nothing** — `check-readable!` runs before the fetch |

`parquet.bytes/counting` records every range, and the tests assert on bytes
fetched rather than on answers: a scan that pruned two of three row groups and
one that read all three return the same rows, so only the byte accounting can
tell them apart.

On the checked-in fixture the numbers are unflattering — 1,534 bytes to open a
2,427-byte file, because a footer costs roughly the same whether it describes
nine rows or nine billion. That is the shape of the win, not a counterexample
to it: footer size is O(row groups × columns) and data is O(rows), so the
fraction collapses on any file worth ranging. What the tests pin is the
structural claim — opening never touches a data page, and a column read
fetches that chunk's own size and no more.

`prefetched` is for hosts where a range fetch is asynchronous (a Worker doing
HTTP Range): fetch the ranges `footer-ranges` names, hand them back, parse
without the file. The seam is synchronous because `columnar/IColumnSource` is,
and an async rewrite of the engine is a bigger decision than this repo should
make on its own.

## Layout

| ns | what |
|---|---|
| `parquet.thrift` | Thrift **compact protocol** decoder — Parquet metadata is written in it, so nothing is readable until this is. Self-contained byte grammar, no Parquet concepts. |
| `parquet.footer` | `FileMetaData`: schema, row groups, column chunks, statistics. Two range reads locate everything about a file of any size. |
| `parquet.decode` | Definition levels (RLE / bit-packing hybrid), PLAIN values, dictionary indices, codec dispatch. |
| `parquet.snappy` | Raw snappy decompression. |
| `parquet.bytes` | The byte-range seam: `IByteSource`, plus `counting` and `prefetched`. |
| `parquet.source` | The `IColumnSource`. |

## Test

```sh
clojure -M:test                                          # JVM
nbb --classpath "src:test:$(clojure -Spath)" test/run.cljs
clojure -M:cljs -m cljs.main --target node -m parquet.cljs-runner
clojure -M:lint
```

All of them. The two ClojureScript runs found three real defects in this repo
that the JVM suite did not: Node's Buffer pooling (reading a small file
returned the allocator's slab, failing as "missing leading PAR1"), the
precision refusal firing in the wrong layer, and the integer rounding above.
