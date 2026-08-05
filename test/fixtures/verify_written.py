"""Check that files THIS REPO wrote are valid Parquet, using the reference reader.

    clojure -M:emit /tmp/parquet-out
    .venv/bin/python test/fixtures/verify_written.py /tmp/parquet-out

Why this exists as a separate step: "the writer produces valid Parquet" is a
statement about what a real Parquet implementation accepts, and it cannot be
checked from inside the repo. Round-tripping our writer through our own reader
proves only that the two share an opinion -- and the failures that matter most
are exactly the ones both halves would share. Two were measured while building
this writer, and BOTH passed every in-repo test:

  1. `ColumnChunk.file_offset` omitted. It is `required` in the thrift IDL and
     our reader never looks at it, so the file round-tripped perfectly here and
     pyarrow could not deserialize the footer at all.

  2. `FileMetaData.column_orders` omitted. The file opened, every value was
     correct -- and every statistic read back as None, because the spec forbids
     trusting `min_value`/`max_value` when no sort order is declared. Since
     Parquet was chosen over a sidecar precisely so that OTHER tools could use
     the statistics, this was a total failure of the writer's purpose that was
     invisible from inside the repo.

So the statistics assertions below are not decoration. They are the point.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pathlib
import sys

out = pathlib.Path(sys.argv[1])

EXPECTED = {
    "three-row-groups": {
        "price":  [10, 20, 30, 110, 120, 130, 210, 220, 230],
        "region": ["east", "east", "west", "west", "east", "east", "east", "west", "west"],
        "note":   [None, "clearance", None, None, None, "sale", None, None, None],
        "ratio":  [1.5, 2.25, None, -0.5, 0.0, 3.75, None, 1.0, -2.5],
    },
    "single-row-group": {
        "price":  [10, 20, 30, 110, 120, 130, 210, 220, 230],
        "region": ["east", "east", "west", "west", "east", "east", "east", "west", "west"],
        "note":   [None, "clearance", None, None, None, "sale", None, None, None],
        "ratio":  [1.5, 2.25, None, -0.5, 0.0, 3.75, None, 1.0, -2.5],
    },
    "big-ints": {"b": [4611686018427387905, -4611686018427387905, 0]},
    "all-null": {"v": [None] * 4, "s": [None] * 4},
    "no-nulls": {"n": [1, 2, 3], "s": ["a", "bb", "ccc"]},
    "empty":    {"n": []},
    "unicode":  {"s": ["日本語", "", "aéb", None]},
}

ROW_GROUPS = {"three-row-groups": 3}

# The whole reason for writing Parquet rather than a sidecar: another tool must
# be able to read the statistics. Per row group, for one named column.
STATS = {
    "three-row-groups": ("price", [(10, 30, 0), (110, 130, 0), (210, 230, 0)]),
    "no-nulls":         ("n",     [(1, 3, 0)]),
    "big-ints":         ("b",     [(-4611686018427387905, 4611686018427387905, 0)]),
}

failures = []
for name, expected in EXPECTED.items():
    path = out / f"{name}.parquet"
    try:
        f = pq.ParquetFile(path)
        table = f.read()
        # Walks offsets and buffer bounds rather than trusting the metadata.
        table.validate(full=True)

        if name in ROW_GROUPS and f.num_row_groups != ROW_GROUPS[name]:
            failures.append(f"{name}: {f.num_row_groups} row groups, "
                            f"expected {ROW_GROUPS[name]}")

        got_cols = list(table.column_names)
        if got_cols != list(expected.keys()):
            failures.append(f"{name}: columns {got_cols} != {list(expected.keys())}")

        for col, want in expected.items():
            got = table.column(col).to_pylist()
            if got != want:
                failures.append(f"{name}.{col}: {got!r} != {want!r}")

        if name in STATS:
            col, per_group = STATS[name]
            idx = got_cols.index(col)
            for rg, (lo, hi, nulls) in enumerate(per_group):
                s = f.metadata.row_group(rg).column(idx).statistics
                if s is None:
                    failures.append(
                        f"{name} rg{rg}.{col}: NO STATISTICS VISIBLE -- the file "
                        f"opens and reads correctly, and every pruning decision "
                        f"another tool would make is lost")
                elif (s.min, s.max, s.null_count) != (lo, hi, nulls):
                    failures.append(
                        f"{name} rg{rg}.{col}: stats {(s.min, s.max, s.null_count)} "
                        f"!= {(lo, hi, nulls)}")

        # An all-null column must report null_count and NO bounds: min/max of
        # nothing is absent, not a large number.
        if name == "all-null":
            s = f.metadata.row_group(0).column(0).statistics
            if s is not None and s.min is not None:
                failures.append(f"all-null: reported a min ({s.min}) for a column "
                                f"with no values")

        print(f"ok   {name}  ({f.num_row_groups} row group(s), {table.num_rows} rows)")
    except Exception as e:
        failures.append(f"{name}: {type(e).__name__}: {e}")
        print(f"FAIL {name}: {e}")

if failures:
    print("\n" + "\n".join(failures))
    sys.exit(1)
print(f"\nall {len(EXPECTED)} written files accepted by pyarrow {pa.__version__}, "
      f"statistics visible")
