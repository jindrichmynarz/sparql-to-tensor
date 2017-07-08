(ns sparql-to-tensor.core
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [stencil.core :as stencil]
            [sparclj.core :as sparql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.avl :as avl])
  (:import (java.io File)
           (java.util Locale)
           (java.text DecimalFormat)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (org.la4j.matrix.sparse CRSMatrix)))

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
  (letfn [(render-query [template]
            (fn [[limit offset]]
              (stencil/render-string template {:limit limit :offset offset})))
          (fetch [query-fn]
            (sparql/select-paged endpoint query-fn ::sparql/parallel? true))]
    (lazy-cat' (map (comp fetch render-query) queries))))

(defn write-relations
  "Write `relations` to a temporary GZipped CSV file.
  Returns the file."
  [relations]
  (let [tmpfile (File/createTempFile "relations" ".csv.gz")
        vectorize (juxt (comp iri->local-name :feature) :s :o :weight)]
    (with-open [writer (-> tmpfile io/output-stream GZIPOutputStream. io/writer)]
      (csv/write-csv writer (map vectorize relations)))
    tmpfile))

(defn build-entity-index
  "Build index of entities from relations in GZipped CSV `tmpfile`.
  Returns a map with :entity-index and :features."
  [tmpfile]
  (let [entity-index (transient (avl/sorted-set))
        features (transient (avl/sorted-set))]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (doseq [[feature s o _] (csv/read-csv reader)]
        (conj! entity-index s)
        (conj! entity-index o)
        (conj! features feature)))
    {:entity-index (persistent! entity-index)
     :features (persistent! features)}))

(defn write-index
  "Write `index` into the `output-dir`."
  [output-dir index]
  (with-open [writer (io/writer (File. output-dir "headers.txt"))]
    (csv/write-csv writer (map vector index))))

(defn init-matrices
  "For each feature in `features` initialize a square matrix of size given by `entity-count`.
  Return a map of the matrices keyed by features."
  [^Integer entity-count
   features]
  (letfn [(->matrix [] (CRSMatrix. entity-count entity-count))]
    (reduce (fn [matrices feature] (assoc matrices feature (->matrix))) {} features)))

(defn populate-matrices!
  "Populate `matrices` with relations read from in a GZipped CSV `tmpfile`,
  where entities are translated to indices via `entity-index`."
  [matrices entity-index tmpfile]
  (let [entity->index (fn [^String entity] (avl/rank-of entity-index entity))]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (doseq [[feature s o weight] (csv/read-csv reader)
              :let [^int s-index (entity->index s)
                    ^int o-index (entity->index o)
                    ^CRSMatrix matrix (get matrices feature)
                    ^double weight' (or (->double weight) 1.0)]]
        (.set matrix s-index o-index weight')))
    ; The temporary file is no longer needed after this point.
    (.delete tmpfile)))

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
  "Build tensor slices from relations retrieved by SPARQL `queries`.
  The slices are written to `output-dir`.
  Each slice is written into a {feature local name}.mtx MatrixMarket file.
  Header containing the index of entity IRIs is written to header.txt file."
  [output-dir queries]
  (let [tmpfile (-> queries fetch-relations write-relations)
        {:keys [entity-index features]} (build-entity-index tmpfile)
        matrices (init-matrices (count entity-index) features)]
    (write-index output-dir entity-index)
    (populate-matrices! matrices entity-index tmpfile)
    (serialize-matrices output-dir matrices)))
