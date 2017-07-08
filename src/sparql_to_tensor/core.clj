(ns sparql-to-tensor.core
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [stencil.core :as stencil]
            [sparclj.core :as sparql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.core.reducers :as r]
            [clojure.set :refer [union]]
            [clojure.data.avl :as avl])
  (:import (java.io File)
           (java.util ArrayList Locale)
           (java.text DecimalFormat)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (org.la4j.matrix.sparse CRSMatrix)))

(set! *warn-on-reflection* true)

(defn- ->double
  "Attempt to parse string `s` to double."
  [^String s]
  (when (seq s) (Double/parseDouble s)))

(defn- iri->local-name
  "Extracts local name from `iri`.
  Expects the local name to be the part of `iri` after the last slash or hash."
  [^String iri]
  (string/replace iri #"^.+(?:#|/)(.+)$" "$1"))

(defn- lazy-cat'
  "Lazily concatenates a sequences `colls`.
  Taken from <http://stackoverflow.com/a/26595111/385505>."
  [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(defn fetch-relations
  "Fetch relations from results of SPARQL `queries`."
  [queries]
  (letfn [(render-query [[limit offset]] (stencil/render-string template {:limit limit :offset offset}))
          (fetch [query-fn] (sparql/select-paged endpoint query-fn ::sparql/parallel? true))]
    (lazy-cat' (map (comp fetch render-query) queries))))

(defn write-relations
  "Write `relations` to a temporary GZipped CSV file.
  Returns the file."
  [relations]
  (let [tmpfile (doto (File/createTempFile "relations" ".csv.gz") (.deleteOnExit))
        vectorize (juxt (comp iri->local-name :feature) :s :o :weight)]
    (with-open [writer (-> tmpfile io/output-stream GZIPOutputStream. io/writer)]
      (->> relations
           (map vectorize)
           (csv/write-csv writer)))
    tmpfile))

(defn build-entity-index
  "Build index of entities from relations in GZipped CSV `tmpfile`.
  Returns a map with :entity-index and :features."
  [tmpfile]
  (letfn [(relation->index
            [{:keys [entity-index features]}
             [feature s o _]]
            (-> entity-index
                (conj! s)
                (conj! o))
            {:entity-index entity-index
             :features (conj features feature)})]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (-> (reduce relation->index
                  {:entity-index (transient (avl/sorted-set)) :features #{}}
                  (csv/read-csv reader))
          (update :entity-index (comp persistent!))))))

(defn write-index
  "Write `index` into the `output-dir`."
  [output-dir index]
  (with-open [writer (io/writer (File. output-dir "headers.txt"))]
    (csv/write-csv writer (map vector index))))

(defn init-matrices
  "For each feature in `features` initialize a square matrix of size given by `entities-size`.
  Return a map of the matrices keyed by features."
  [^Integer entities-size
   features]
  (letfn [(->matrix [] (CRSMatrix. entities-size entities-size))]
    (reduce (fn [matrices feature] (assoc matrices feature (->matrix))) {} features)))

(defn relations->csv
  [output-dir
   ^ArrayList entity-index
   tmpfile]
  (let [entity->index (fn [^String entity] (avl/rank-of entity-index entity))
        index-relation (fn [[_ s o weight]]
                         [(entity->index s)
                          (entity->index o)
                          (or (->double weight) 1.0)])]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (doseq [feature-relations (partition-by first (take 10000 (csv/read-csv reader)))
              :let [feature (ffirst feature-relations)]]
        (with-open [writer (io/writer (File. output-dir (str feature ".csv")))]
          (csv/write-csv writer (map index-relation feature-relations)))))))

(defn csv->matrix
  [csv-file]
  (CRSMatrix/fromCSV (slurp csv-file)))

(defn populate-matrices!
  "Populate `matrices` with relations read from in a GZipped CSV `tmpfile`,
  where entities are translated to indices via `entity-index`."
  ; FIXME: `tmpfile` can be deleted after this point.
  [matrices entity-index tmpfile]
  (let [entity->index (fn [^String entity] (avl/rank-of entity-index entity))
        entity->index' (memoize entity->index)]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (doseq [[feature s o weight] (take 10000 (csv/read-csv reader))
              :let [^int s-index (entity->index' s)
                    ^int o-index (entity->index' o)
                    ^CRSMatrix matrix (get matrices feature)
                    ^double weight' (or (->double weight) 1.0)]]
        (.set matrix s-index o-index weight')))))

(defn serialize-matrices
  "Serialize `matrices` in the MatrixMarker format to `output-dir`."
  [output-dir matrices]
  (let [number-format (DecimalFormat/getInstance Locale/US)
        serialize-matrix (fn [matrix]
                           (-> matrix
                               (.toMatrixMarket number-format)
                               (string/replace " row-major" "")))]
    (doseq [[feature matrix] matrices
            :let [output-file (File. output-dir (str feature ".mtx"))]]
      (spit output-file (serialize-matrix matrix)))))

(defn sparql->tensor
  [output-dir queries]
  (let [tmpfile (-> queries fetch-relations write-relations)
        {:keys [entity-index features]} (build-entity-index tmpfile)
        matrices (init-matrices (count entity-index) features)]
    (write-index output-dir entity-index)
    (populate-matrices! matrices entity-index tmpfile)
    (serialize-matrices output-dir matrices)))
