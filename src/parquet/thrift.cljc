(ns parquet.thrift
  "Thrift compact protocol decoder — the format Parquet's metadata is written in.

  Parquet's footer is a Thrift `FileMetaData` struct in the *compact*
  protocol, so nothing about a Parquet file is readable until this is. It is
  its own namespace because it has nothing to do with columns: it is a
  self-contained byte grammar, and keeping it separate is what lets it be
  tested against byte sequences taken from the protocol spec rather than
  against this repo's idea of a Parquet file.

  Reads from a vector of unsigned ints. That is a portable representation
  rather than a fast one — a typed array would avoid the boxing — and the
  choice is stated here because the profile that reveals it would otherwise
  arrive as a surprise. Every reader threads an explicit position, so the
  decoder is pure."
  (:refer-clojure :exclude [bytes]))

(defn le-uint
  "Unsigned little-endian integer of `n` bytes, EXACTLY or not at all.

  The obvious spelling — `(+ acc (* byte (Math/pow 2 (* 8 k))))` — is wrong
  for int64. 2^56 times a byte exceeds 2^53, so the top two bytes of a 64-bit
  value land beyond double precision and the answer comes back quietly
  rounded. It is quiet on the JVM too, because `Math/pow` returns a double
  and the sum is a double before anything casts it back.

  So: exact integer arithmetic on the JVM, and on ClojureScript — where
  numbers ARE doubles — a value past `MAX_SAFE_INTEGER` is refused rather
  than rounded. A reader that returns a plausible wrong number is worse than
  one that says it cannot represent this value, because nothing downstream
  can tell."
  [bs i n]
  #?(:clj  (loop [k 0 acc 0]
             (if (= k n)
               acc
               (recur (inc k) (bit-or acc (bit-shift-left (long (nth bs (+ i k)))
                                                          (* 8 k))))))
     :cljs (let [acc (loop [k 0 acc 0]
                       (if (= k n)
                         acc
                         (recur (inc k) (+ acc (* (nth bs (+ i k))
                                                  (Math/pow 2 (* 8 k)))))))]
             (if (> acc js/Number.MAX_SAFE_INTEGER)
               (throw (ex-info "integer exceeds this runtime's exact range"
                               {:type :parquet/precision-unavailable
                                :bytes n :approx acc}))
               acc))))

;; type ids in a field header's low nibble
(def ^:private t-stop 0)
(def types
  {0 :stop 1 :true 2 :false 3 :byte 4 :i16 5 :i32 6 :i64
   7 :double 8 :binary 9 :list 10 :set 11 :map 12 :struct})

(defn- u8 [bs i] (nth bs i))

(defn read-varint
  "ULEB128. -> [value next-index].

  Bounded at 10 bytes: a varint longer than that cannot be a valid i64 and is
  a corrupt or hostile file trying to make the reader loop."
  [bs i]
  (loop [i i shift 0 acc 0 n 0]
    (when (> n 10)
      (throw (ex-info "varint too long" {:type :parquet/malformed :at i})))
    (let [b (u8 bs i)
          acc (+ acc (* (bit-and b 0x7f) (Math/pow 2 shift)))]
      (if (zero? (bit-and b 0x80))
        [acc (inc i)]
        (recur (inc i) (+ shift 7) acc (inc n))))))

(defn zigzag
  "Compact protocol encodes signed integers zigzag, so that small negatives
  are small varints."
  [n]
  (let [n (long n)]
    (if (zero? (bit-and n 1))
      (quot n 2)
      (- (- (quot n 2)) 1))))

(defn read-i64 [bs i] (let [[v i'] (read-varint bs i)] [(zigzag v) i']))
(def read-i32 read-i64)

(defn read-double
  "8 bytes, little-endian IEEE 754. Goes through the raw bit pattern rather
  than `le-uint`, because a double's bits routinely exceed the exact integer
  range that would refuse."
  [bs i]
  [#?(:clj (Double/longBitsToDouble
            (loop [k 0 acc 0]
              (if (= k 8)
                acc
                (recur (inc k) (bit-or acc (bit-shift-left (long (u8 bs (+ i k)))
                                                           (* 8 k)))))))
      :cljs (let [a (js/Uint8Array. 8)]
              (dotimes [k 8] (aset a k (u8 bs (+ i k))))
              (.getFloat64 (js/DataView. (.-buffer a)) 0 true)))
   (+ i 8)])

(defn read-binary
  "varint length, then that many raw bytes. -> [byte-vector next-index]"
  [bs i]
  (let [[n i'] (read-varint bs i)
        n (long n)]
    [(subvec (vec bs) i' (+ i' n)) (+ i' n)]))

(defn bytes->string
  "UTF-8 decode. Parquet stores column names and BYTE_ARRAY statistics as raw
  bytes, and a caller who wants the string has to say so — the same bytes are
  a string in `path_in_schema` and an opaque value in a FIXED_LEN column."
  [bv]
  #?(:clj (String. (byte-array (mapv unchecked-byte bv)) "UTF-8")
     :cljs (.decode (js/TextDecoder. "utf-8") (js/Uint8Array. (clj->js (vec bv))))))

(declare read-value skip-value)

(defn- read-collection-header [bs i]
  (let [b (u8 bs i)
        size (bit-shift-right b 4)
        etype (get types (bit-and b 0x0f))]
    (if (= size 15)
      (let [[n i'] (read-varint bs (inc i))] [(long n) etype i'])
      [size etype (inc i)])))

(defn read-list [bs i]
  (let [[n etype i'] (read-collection-header bs i)]
    (loop [k 0 i i' acc []]
      (if (= k n)
        [acc i]
        (let [[v i''] (read-value bs i etype)]
          (recur (inc k) i'' (conj acc v)))))))

(defn read-struct
  "-> [{field-id value} next-index]. Field ids rather than names: the names
  live in `parquet.footer`, next to the meaning, so this namespace stays a
  pure protocol decoder."
  [bs i]
  (loop [i i last-id 0 acc {}]
    (let [b (u8 bs i)
          tid (bit-and b 0x0f)]
      (if (= tid t-stop)
        [acc (inc i)]
        (let [delta (bit-shift-right b 4)
              [fid i'] (if (zero? delta)
                         (let [[z i''] (read-varint bs (inc i))] [(zigzag z) i''])
                         [(+ last-id delta) (inc i)])
              ftype (get types tid)
              [v i''] (read-value bs i' ftype)]
          (recur i'' fid (assoc acc fid v)))))))

(defn read-value [bs i type]
  (case type
    :true   [true i]
    :false  [false i]
    :byte   [(u8 bs i) (inc i)]
    (:i16 :i32 :i64) (read-i64 bs i)
    :double (read-double bs i)
    :binary (read-binary bs i)
    (:list :set) (read-list bs i)
    :struct (read-struct bs i)
    :map    (throw (ex-info "thrift map is not used by Parquet metadata"
                            {:type :parquet/unsupported-thrift :at i}))
    (throw (ex-info (str "unknown thrift type " (pr-str type))
                    {:type :parquet/malformed :at i}))))

(defn parse-struct
  "Decode one compact-protocol struct starting at `i`."
  [bs i]
  (first (read-struct bs i)))

(defn parse-struct-with-end [bs i] (read-struct bs i))
