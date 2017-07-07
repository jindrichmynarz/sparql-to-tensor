(ns sparql-to-tensor.scratchpad
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [sparql-to-tensor.spec :as spec]
            [sparql-to-tensor.core :as core]
            [clojure.java.io :as io]
            [mount.core :as mount]
            [sparclj.core :as sparql])
  (:import (java.io File)))

(comment
  (def output
    (let [dir (io/as-file "/tmp/tensors")]
      (when-not (.exists dir) (.mkdir dir))
      dir))

  (def queries
    (map (comp slurp io/resource) ["contract_properties.mustache" "bidder_properties.mustache"]))

  (def params {::sparql/url "http://lod2-dev.vse.cz:8890/sparql"
               ::sparql/page-size 1000
               ::sparql/parallel? true
               ::spec/queries queries
               ::spec/output output})

  (mount/start-with-args params)
  (mount/stop)

  (stencil.loader/set-cache (clojure.core.cache/ttl-cache-factory {} :ttl 0))
  (time (def relations (doall (core/fetch-relations queries))))
  (time (def tmpfile (core/write-relations relations)))
  (time (def data (core/build-entity-index tmpfile)))
  (core/write-index output (:index data))
  (def matrices (core/init-matrices (count (:index data)) (:features data)))
  (core/populate-matrices! matrices (:index data) tmpfile)
  (core/serialize-matrices output matrices)
  (time (core/sparql->tensor-multipass output queries))
  )
