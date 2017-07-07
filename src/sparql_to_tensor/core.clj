(ns sparql-to-tensor.core
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [stencil.core :as stencil]
            [sparclj.core :as sparql]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.core.reducers :as r]
            [clojure.set :refer [union]])
  (:import (java.io File)
           (java.util ArrayList Locale)
           (java.text DecimalFormat)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (org.la4j.matrix.sparse CCSMatrix)))

(defn- ->double
  "Attempt to parse string `s` to double."
  [^String s]
  (when (seq s) (Double/parseDouble s)))

(defn- iri->local-name
  "Extracts local name from `iri`.
  Expects the local name to be the part of `iri` after the last slash or hash."
  [^String iri]
  (string/replace iri #"^.+(?:#|/)(.+)$" "$1"))

(defn sparql->tensor
  "Retrieve results of `queries` and convert them into tensor slices output to `output` directory."
  [queries output]
  (let [entities (ArrayList.)
        slices (atom {})
        entity->index! (fn [iri]
                        (let [index (.indexOf entities iri)]
                          ; Matrices are 1-offset, hence inc.
                          (inc (if (not= index -1)
                            index 
                            (.size (doto entities (.add iri)))))))
        add-relation! (fn 
                        ([s o feature weight]
                         #_(if-let [slice (get @slices feature)]
                           (add-relation! slice s o feature weight))))]
    (doseq [query queries
            :let [get-query-fn (fn [[limit offset]]
                                 (stencil/render-string query {:limit limit :offset offset}))]]
      (doseq [{:keys [s o feature weight]
               :or {weight 1}} (sparql/select-paged endpoint get-query-fn)
              :let [s-index (entity->index! s)
                    o-index (entity->index! o)
                    feature-local-name (iri->local-name feature)]]
        ))))

; Functional, potentially slow solution
; TODO:
; - Start with naive sequence processing
; - Graduate to transducers

(defn- ->query-fn
  "Make a function to render paged queries from `template`."
  [template]
  (fn [[limit offset]]
    (stencil/render-string template {:limit limit :offset offset})))

(defn- lazy-cat'
  "Lazily concatenates a sequences `colls`.
  Taken from <http://stackoverflow.com/a/26595111/385505>."
  [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(defn sparql->tensor'
  [queries output]
  (let [fetch-fn (comp (partial sparql/select-paged endpoint) ->query-fn)
        relations (lazy-cat' (map fetch-fn queries))
        entity->index (fn [entities entity])]
    (reduce (fn [{:keys [entities slices]
                  :as data}
                 {:keys [s o feature weight]
                  :or {weight 1}}]
              (let [feature-local-name (iri->local-name feature)]))
            {:entities []
             :slices {}}
            relations)))

; Multipass version

(defn fetch-relations
  "Fetch relations from results of SPARQL `queries`."
  [queries]
  (letfn [(fetch [query-fn] (sparql/select-paged endpoint query-fn ::sparql/parallel? true))]
    (lazy-cat' (map (comp fetch ->query-fn) queries))))

(defn write-relations
  "Write `relations` to a temporary GZipped CSV file.
  Returns the file."
  [relations]
  (let [tmpfile (doto (File/createTempFile "relations" ".csv.gz") (.deleteOnExit))
        vectorize (juxt :s :o (comp iri->local-name :feature) :weight)]
    (with-open [writer (-> tmpfile io/output-stream GZIPOutputStream. io/writer)]
      (csv/write-csv writer (map vectorize relations)))
    tmpfile))

(defn build-entity-index
  "Build index of entities from relations in GZipped CSV `tmpfile`.
  Returns a map with :entity-index and :features."
  [tmpfile]
  (letfn [(relation->index
            [{:keys [entity-index features]}
             [s o feature _]]
            (-> entity-index
                (conj! s)
                (conj! o))
            {:entity-index entity-index
             :features (conj features feature)})]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (-> (reduce relation->index {:entity-index (transient #{}) :features #{}} (csv/read-csv reader))
          (update :entity-index (comp vec persistent!))))))

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
  (letfn [(->matrix [] (CCSMatrix. entities-size entities-size))]
    (reduce (fn [matrices feature] (assoc matrices feature (->matrix))) {} features)))

(defn populate-matrices!
  "Populate `matrices` with relations read from in a GZipped CSV `tmpfile`,
  where entities are translated to indices via `entity-index`."
  ; FIXME: `tmpfile` can be deleted after this point.
  [matrices entity-index tmpfile]
  (letfn [(entity->index [entity] (.indexOf entity-index entity))]
    (with-open [reader (-> tmpfile io/input-stream GZIPInputStream. io/reader)]
      (doseq [[s o feature weight] (take 1000 (csv/read-csv reader))
              :let [s-index (entity->index s)
                    o-index (entity->index o)
                    matrix (get matrices feature)
                    weight' (or (->double weight) 1.0)]]
        (.set matrix s-index o-index weight')))))

(defn serialize-matrices
  "Serialize `matrices` in the MatrixMarker format to `output-dir`."
  [output-dir matrices]
  (let [number-format (DecimalFormat/getInstance Locale/US)
        serialize-matrix (fn [matrix]
                           (-> matrix
                               (.toMatrixMarket number-format)
                               (string/replace " column-major" "")))]
    (doseq [[feature matrix] matrices
            :let [output-file (File. output-dir (str feature ".mtx"))]]
      (spit output-file (serialize-matrix matrix)))))

(defn sparql->tensor-multipass
  [output-dir queries]
  (let [tmpfile (-> queries fetch-relations write-relations)
        {:keys [entity-index features]} (build-entity-index tmpfile)
        matrices (init-matrices (count entity-index) features)]
    (write-index output-dir entity-index)
    (populate-matrices! matrices entity-index tmpfile)
    (serialize-matrices output-dir matrices)))
