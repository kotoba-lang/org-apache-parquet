(ns parquet.bytes
  "Where the bytes come from, as a seam rather than an argument.

  The first version of this reader took the whole file as a vector. It was
  correct and it defeated the point: Parquet exists so that a query reads a
  footer and two column chunks instead of forty gigabytes, and a reader that
  demands the forty gigabytes first has thrown that away before it starts.

  So a source answers **ranges**, and every read this library performs is one
  of them. `counting` records what was asked for, which is how a test proves
  the reader touched a column chunk and nothing else — an assertion on the
  *answer* cannot tell that apart from reading everything and slicing.

  ## Synchronous, deliberately

  `columnar/IColumnSource` is synchronous, so this is too. On a host where
  fetching a range is asynchronous — a Worker doing HTTP Range requests — the
  caller pre-fetches into `prefetched` and passes that. `footer-ranges` exists
  so such a caller knows what to fetch before it can parse anything: it is a
  two-round-trip protocol (tail, then footer), and pretending otherwise would
  mean either an async rewrite of the engine or a silent whole-file read."
  (:refer-clojure :exclude [bytes]))

(defprotocol IByteSource
  (-size [src] "Total bytes in the object. Known without reading it — from a
    HEAD, a stat, or the catalog's `:object/size-bytes`.")
  (-read-range [src start end] "Bytes in `[start end)` as a vector of unsigned
    ints. `end` is exclusive."))

(defn of-vector
  "A source over a whole file already in memory. For tests, and for objects
  small enough that ranging them is pointless."
  [v]
  (let [v (vec v)]
    (reify IByteSource
      (-size [_] (count v))
      (-read-range [_ start end] (subvec v start end)))))

(defn prefetched
  "A source over ranges someone else already fetched.

  `chunks` is a seq of `[start bytes]`. A range that no chunk covers throws
  rather than returning short: a decoder handed fewer bytes than it asked for
  produces nonsense, and the caller who under-fetched is the one who can fix
  it."
  [size chunks]
  (let [chunks (vec chunks)]
    (reify IByteSource
      (-size [_] size)
      (-read-range [_ start end]
        (or (some (fn [[at bs]]
                    (let [stop (+ at (count bs))]
                      (when (and (<= at start) (<= end stop))
                        (subvec (vec bs) (- start at) (- end at)))))
                  chunks)
            (throw (ex-info "range was not prefetched"
                            {:type :parquet/missing-range
                             :want [start end]
                             :have (mapv (fn [[at bs]] [at (+ at (count bs))]) chunks)})))))))

(defn counting
  "Wrap a source so every range is recorded.

  `{:ranges [[start end] ..] :bytes n}`. The reason this exists is the same
  reason `columnar.source/counting` does: correct answers prove nothing about
  how much was read to get them."
  [src]
  (let [log (atom {:ranges [] :bytes 0})]
    {:log log
     :source (reify IByteSource
               (-size [_] (-size src))
               (-read-range [_ start end]
                 (swap! log (fn [l] (-> l
                                        (update :ranges conj [start end])
                                        (update :bytes + (- end start)))))
                 (-read-range src start end)))}))

(defn read-counts [c] @(:log c))

(defn source
  "Coerce `x` to an `IByteSource`. A vector is taken as a whole file."
  [x]
  (if (satisfies? IByteSource x) x (of-vector x)))

(def footer-suffix-bytes
  "The fixed tail every Parquet file ends with: a 4-byte footer length and
  `PAR1`."
  8)

(defn footer-ranges
  "The ranges a caller must fetch, in order, to parse the footer of a file of
  `size` bytes.

  The second depends on the first, so this returns a function rather than a
  list: an async caller fetches the tail, hands it back, and is told what to
  fetch next. Stating the dependency in the type is cheaper than discovering
  it as a wrong answer from a single speculative read."
  [size]
  {:tail [(- size footer-suffix-bytes) size]})
