(ns parquet.decode
  "Page decoding: definition levels, then PLAIN values.

  The supported subset is **declared, and everything outside it throws with
  the name of what it met**. A decoder that guesses at an encoding it does
  not know produces plausible numbers, and plausible numbers from a file
  nobody can re-check is the worst failure available to this library — worse
  than not reading the file, because it is silent.

  Supported: `UNCOMPRESSED` codec, `PLAIN` encoding, `DATA_PAGE` (v1), flat
  schemas, `REQUIRED` and `OPTIONAL` columns. Not supported, by name:
  dictionary encodings, delta encodings, byte-stream-split, v2 data pages,
  every compression codec, and repeated (nested) columns."
  (:require [parquet.thrift :as th]))

(def ^:private le th/le-uint)

(defn- as-signed-64 [v]
  (let [v (long v)] v))

(defn bit-width
  "Bits needed to hold `max-level`. 0 when the column is REQUIRED, in which
  case the levels section is absent entirely rather than present and empty."
  [max-level]
  (loop [w 0 n max-level] (if (zero? n) w (recur (inc w) (bit-shift-right n 1)))))

(defn rle-hybrid
  "The RLE / bit-packing hybrid Parquet uses for levels and dictionary indices.

  -> a vector of at least `n` values (a bit-packed run always carries a
  multiple of 8, so the tail is trimmed by the caller)."
  [bs i end width n]
  (if (zero? width)
    (vec (repeat n 0))
    (let [byte-width (quot (+ width 7) 8)]
      (loop [i i acc []]
        (if (or (>= (count acc) n) (>= i end))
          acc
          (let [[header i'] (th/read-varint bs i)
                header (long header)]
            (if (zero? (bit-and header 1))
              ;; RLE run: `count` repeats of one value
              (let [run (bit-shift-right header 1)
                    v (long (le bs i' byte-width))]
                (recur (+ i' byte-width) (into acc (repeat run v))))
              ;; bit-packed run: groups of 8 values, LSB-first within a byte
              (let [groups (bit-shift-right header 1)
                    total (* groups 8)
                    nbytes (quot (* total width) 8)
                    vals (loop [k 0 out []]
                           (if (= k total)
                             out
                             (let [bit (* k width)
                                   v (loop [b 0 v 0]
                                       (if (= b width)
                                         v
                                         (let [pos (+ bit b)
                                               byte (nth bs (+ i' (quot pos 8)))
                                               on (bit-and (bit-shift-right byte (mod pos 8)) 1)]
                                           (recur (inc b) (bit-or v (bit-shift-left on b))))))]
                               (recur (inc k) (conj out v)))))]
                (recur (+ i' nbytes) (into acc vals))))))))))

(defn plain-values
  "Decode `n` PLAIN-encoded values of `physical` starting at `i`."
  [bs i n physical]
  (loop [k 0 i i acc []]
    (if (= k n)
      acc
      (case physical
        :int64 (recur (inc k) (+ i 8) (conj acc (as-signed-64 (le bs i 8))))
        :int32 (recur (inc k) (+ i 4) (conj acc (long (le bs i 4))))
        :double (let [[v i'] (th/read-double bs i)] (recur (inc k) i' (conj acc v)))
        :byte-array (let [len (long (le bs i 4))
                          start (+ i 4)]
                      (recur (inc k) (+ start len)
                             (conj acc (th/bytes->string (subvec (vec bs) start (+ start len))))))
        (throw (ex-info (str "PLAIN decoding of " (name physical) " is not implemented")
                        {:type :parquet/unsupported-type :physical physical}))))))

(defn- refuse! [what value extra]
  (throw (ex-info (str "parquet: unsupported " what " " (pr-str value)
                       " — statistics are still readable from this file")
                  (merge {:type :parquet/unsupported what value} extra))))

(defn data-page
  "Decode one v1 DATA_PAGE into `{:values [..] :valid [..]}`.

  `max-def-level` is 1 for an OPTIONAL flat column and 0 for a REQUIRED one.
  A value is present exactly when its definition level equals the maximum;
  PLAIN values are written only for the present rows, which is why the levels
  have to be decoded first even to know how many values to read."
  [bs body-start {:keys [num-values encoding definition-level-encoding]} physical max-def-level]
  (when-not (= :plain encoding)
    (refuse! :encoding encoding {:supported [:plain]}))
  (if (zero? max-def-level)
    {:values (plain-values bs body-start num-values physical)
     :valid (vec (repeat num-values true))}
    (do
      (when-not (contains? #{:rle :bit-packed} definition-level-encoding)
        (refuse! :definition-level-encoding definition-level-encoding {}))
      (let [;; v1 pages prefix the levels section with its byte length as a
            ;; 4-byte LE int, which is what makes the values' start findable
            ;; without decoding the levels first.
            len (long (le bs body-start 4))
            levels-start (+ body-start 4)
            levels (take num-values
                         (rle-hybrid bs levels-start (+ levels-start len)
                                     (bit-width max-def-level) num-values))
            valid (mapv #(= % max-def-level) levels)
            present (count (filter true? valid))
            vs (plain-values bs (+ levels-start len) present physical)]
        {:values (loop [i 0 vs vs out []]
                   (if (= i (count valid))
                     out
                     (if (nth valid i)
                       (recur (inc i) (rest vs) (conj out (first vs)))
                       (recur (inc i) vs (conj out nil)))))
         :valid valid}))))

(defn check-readable!
  "Throw unless this column chunk is inside the supported subset.

  Called before any byte of the chunk is fetched, so a refusal costs one
  metadata lookup rather than a download."
  [{:keys [codec encodings dictionary-page-offset path]}]
  (when-not (= :uncompressed codec)
    (refuse! :codec codec {:column path :supported [:uncompressed]}))
  (when dictionary-page-offset
    (refuse! :encoding :dictionary {:column path}))
  (when-let [bad (seq (remove #{:plain :rle :bit-packed} encodings))]
    (refuse! :encoding (vec bad) {:column path :supported [:plain]}))
  true)
