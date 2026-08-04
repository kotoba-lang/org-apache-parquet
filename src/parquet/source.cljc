(ns parquet.source
  "A Parquet file as a `columnar/IColumnSource`.

  The split `columnar.source` insists on — cheap statistics, expensive reads
  — is not an accommodation here; it is what a Parquet file already is. A row
  group's min/max live in the footer, so `-chunk-stats` costs nothing beyond
  the metadata already parsed, while `-read-column` has to reach the pages.

  **`-chunk-stats` works on files `-read-column` refuses.** Encoding and
  compression are properties of the pages; statistics are not. So a snappy
  dictionary-encoded file — which this reader cannot decode — still prunes,
  still answers `count`, still answers `min`/`max`. There is a fixture for
  exactly that, because it is the difference between a reader that is
  partially useful and one that is useless until complete."
  (:require [columnar.source :as csrc]
            [columnar.vector :as cvec]
            [parquet.decode :as decode]
            [parquet.footer :as footer]))

(defn- column-index [meta column]
  (or (first (keep-indexed (fn [i c] (when (= column c) i)) (:columns meta)))
      (throw (ex-info "column not in this file"
                      {:type :parquet/unknown-column
                       :column column :columns (:columns meta)}))))

(defn- chunk-meta [meta chunk column]
  (nth (:columns (nth (:row-groups meta) chunk)) (column-index meta column)))

(defn- max-def-level
  "1 for an OPTIONAL flat column, 0 for REQUIRED.

  Flat only: a REPEATED element means repetition levels and a nested
  assembly this reader does not implement, and it is refused by name rather
  than silently flattened."
  [meta column]
  (let [el (first (filter #(= column (:name %)) (rest (:schema meta))))]
    (case (:repetition el)
      :required 0
      :optional 1
      :repeated (throw (ex-info "repeated (nested) columns are not supported"
                                {:type :parquet/unsupported :column column}))
      0)))

(defn open
  "A `columnar/IColumnSource` over the whole file `bs` (a vector of unsigned
  ints — the caller decides how the bytes arrived).

  The footer is parsed once, here. Every later `-chunk-stats` is a lookup in
  what was already read."
  [bs]
  (let [meta (footer/parse bs)]
    (reify csrc/IColumnSource
      (-schema [_] (:columns meta))
      (-chunk-count [_] (count (:row-groups meta)))
      (-chunk-rows [_ chunk] (:num-rows (nth (:row-groups meta) chunk)))
      (-chunk-stats [_ chunk column]
        (let [{:keys [statistics num-values]} (chunk-meta meta chunk column)]
          ;; nil when the writer recorded nothing. `columnar.stats` reads that
          ;; as "no claim" and refuses to prune, which is the whole reason it
          ;; must not be papered over with an empty map here.
          (when statistics
            (merge {:rows num-values} statistics))))
      (-read-column [_ chunk column]
        (let [cm (chunk-meta meta chunk column)]
          (decode/check-readable! cm)
          (let [[header body] (footer/page-header bs (:data-page-offset cm))]
            (when-not (= :data-page (:type header))
              (throw (ex-info (str "unsupported first page type " (pr-str (:type header)))
                              {:type :parquet/unsupported :page (:type header)})))
            (let [{:keys [values valid]}
                  (decode/data-page bs body header (:type cm) (max-def-level meta column))]
              (cvec/of (:type cm) values valid))))))))

(defn metadata
  "The parsed footer, for callers that want the schema or the row-group map
  without going through the column source."
  [bs]
  (footer/parse bs))
