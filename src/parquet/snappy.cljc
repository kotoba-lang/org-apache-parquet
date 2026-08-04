(ns parquet.snappy
  "Snappy decompression, raw format — the codec Parquet writes by default.

  Raw and not framed: Parquet compresses each page body on its own and records
  both sizes in the page header, so the stream framing that carries checksums
  and chunk boundaries has no job here. Handing framed bytes to this would
  read the frame header as a length preamble, so the preamble is checked
  against the header's number rather than trusted.

  Decompression only. A compressor is a different program that happens to
  share a format name, and this repo reads files.

  ## The one subtlety

  A copy element may reference bytes it is **currently producing** — an offset
  smaller than the length, which is how snappy encodes a repeating run. So
  copies are byte-at-a-time, reading from the output as it grows. Slicing the
  source range up front is the natural-looking implementation and it silently
  truncates every run longer than its own period.

  Persistent vectors rather than transients or typed arrays: correct and
  portable first. This decodes a page body, not a stream, and the sizes
  involved are page-sized. Swapping the representation is local to this
  namespace."
  (:refer-clojure :exclude [bytes]))

(defn- u8 [bs i] (nth bs i))

(defn- read-varint [bs i]
  (loop [i i shift 0 acc 0]
    (let [b (u8 bs i)
          acc (+ acc (* (bit-and b 0x7f) (Math/pow 2 shift)))]
      (if (zero? (bit-and b 0x80))
        [(long acc) (inc i)]
        (recur (inc i) (+ shift 7) acc)))))

(defn- le [bs i n]
  (loop [k 0 acc 0]
    (if (= k n) acc (recur (inc k) (bit-or acc (bit-shift-left (u8 bs (+ i k)) (* 8 k)))))))

(defn- copy-back
  "Append `len` bytes taken from `off` positions back in `out`.

  Byte-at-a-time, and re-reading `out` each step, because `off` may be less
  than `len`."
  [out off len]
  (let [n (count out)]
    (when (or (<= off 0) (> off n))
      (throw (ex-info "snappy copy offset is outside the output"
                      {:type :parquet/snappy-bad-offset :offset off :produced n})))
    (loop [k 0 o out]
      (if (= k len)
        o
        (recur (inc k) (conj o (nth o (- (count o) off))))))))

(defn- literal [out bs at len]
  (loop [k 0 o out]
    (if (= k len) o (recur (inc k) (conj o (u8 bs (+ at k)))))))

(defn decompress
  "Raw snappy in `bs[start,end)` -> a vector of unsigned ints.

  `expected` is the uncompressed size the page header promised. Checked, not
  ignored: a disagreement means the stream and the metadata describe
  different things, and continuing hands the decoder a buffer whose contents
  look like data."
  ([bs] (decompress bs 0 (count bs) nil))
  ([bs start end expected]
   (let [[declared i0] (read-varint bs start)]
     (when (and expected (not= (long expected) declared))
       (throw (ex-info "snappy preamble disagrees with the page header"
                       {:type :parquet/snappy-length-mismatch
                        :header expected :stream declared})))
     (loop [i i0 out []]
       (if (>= i end)
         (do (when (not= declared (count out))
               (throw (ex-info "snappy output is not the declared length"
                               {:type :parquet/snappy-truncated
                                :declared declared :actual (count out)})))
             out)
         (let [tag (u8 bs i)]
           (case (bit-and tag 3)
             0 (let [n (bit-shift-right tag 2)]
                 (if (< n 60)
                   (let [len (inc n) s (inc i)]
                     (recur (+ s len) (literal out bs s len)))
                   (let [extra (- n 59)
                         len (inc (le bs (inc i) extra))
                         s (+ i 1 extra)]
                     (recur (+ s len) (literal out bs s len)))))
             1 (let [len (+ 4 (bit-and (bit-shift-right tag 2) 7))
                     off (bit-or (bit-shift-left (bit-and (bit-shift-right tag 5) 7) 8)
                                 (u8 bs (inc i)))]
                 (recur (+ i 2) (copy-back out off len)))
             2 (let [len (inc (bit-shift-right tag 2))
                     off (le bs (inc i) 2)]
                 (recur (+ i 3) (copy-back out off len)))
             3 (let [len (inc (bit-shift-right tag 2))
                     off (le bs (inc i) 4)]
                 (recur (+ i 5) (copy-back out off len))))))))))
