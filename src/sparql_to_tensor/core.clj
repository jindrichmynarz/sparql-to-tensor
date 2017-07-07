(ns sparql-to-tensor.core
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [stencil.core :as stencil]
            [sparclj.core :as sparql]
            [clojure.string :as string])
  (:import (java.util ArrayList)
           (org.la4j.matrix.sparse CCSMatrix CRSMatrix)))

(defn- iri->local-name
  "Extracts local name from `iri`.
  Expects the local name to be the part of `iri` after the last slash or hash."
  [^String iri]
  (string/replace iri #"^.+(?:#|/)(.+)$"))

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
