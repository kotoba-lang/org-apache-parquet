"""Regenerate the test fixtures using the REFERENCE implementation.

Python, and not nbb, for one reason: the only trustworthy oracle for a
Parquet decoder is a file written by Parquet's own writer, and that writer is
reachable from this machine through pyarrow. A fixture this repo generated
itself would test the decoder against its own misunderstanding.

    python3 -m venv .venv && .venv/bin/pip install pyarrow
    .venv/bin/python test/fixtures/generate.py
"""
import pyarrow as pa, pyarrow.parquet as pq, pathlib

here = pathlib.Path(__file__).parent

# Three row groups with disjoint price ranges, so chunk pruning is observable,
# and a nullable column so definition levels are exercised.
table = pa.table({
    "price":  pa.array([10, 20, 30, 110, 120, 130, 210, 220, 230], pa.int64()),
    "region": pa.array(["east","east","west","west","east","east","east","west","west"]),
    "note":   pa.array([None,"clearance",None,None,None,"sale",None,None,None]),
    # Beyond 2^53. A decoder that accumulates with Math/pow returns this
    # rounded, on BOTH runtimes, and says nothing.
    "big":    pa.array([2**62 + 1] * 9, pa.int64()),
})

# plain.parquet — the subset this reader decodes: PLAIN, uncompressed.
pq.write_table(table, here / "plain.parquet", row_group_size=3,
               compression="none", use_dictionary=False,
               write_statistics=True, version="2.6",
               data_page_version="1.0")

# dictionary-snappy.parquet — what pyarrow writes by DEFAULT, and what most
# real files therefore are. Decoded, and asserted to agree value-for-value
# with plain.parquet: two independent encodings of the same data.
pq.write_table(table, here / "dictionary-snappy.parquet", row_group_size=3,
               compression="snappy", use_dictionary=True,
               write_statistics=True, version="2.6")

# gzip.parquet — deliberately OUTSIDE the supported subset, and it has to stay
# that way. Statistics must remain readable from a file this reader cannot
# decode, because the footer does not depend on how the pages were encoded;
# that property is what makes pruning and min/max/count work regardless. When
# snappy became supported, this fixture is what kept the property tested
# instead of quietly losing its only witness.
pq.write_table(table, here / "gzip.parquet", row_group_size=3,
               compression="gzip", use_dictionary=True,
               write_statistics=True, version="2.6")

for p in sorted(here.glob("*.parquet")):
    print(p.name, p.stat().st_size, "bytes")
