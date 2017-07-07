(ns sparql-to-tensor.scratchpad
  (:require [sparql-to-tensor.endpoint :refer [endpoint]]
            [sparql-to-tensor.spec :as spec]
            [sparql-to-tensor.core :as core]
            [clojure.java.io :as io]
            [mount.core :as mount]
            [sparclj.core :as sparql]))

(comment
  (def output
    (let [dir (io/as-file "/tmp/tensors")]
      (when-not (.exists dir) (.mkdir dir))
      dir))

  (def queries
    (map (comp slurp io/resource) ["contract_properties.mustache" "bidder_properties.mustache"]))

  (def params {::sparql/url "http://lod2-dev.vse.cz:8890/sparql"
               ::sparql/page-size 100
               ::spec/queries queries
               ::spec/output output})

  (mount/start-with-args params)
  (take 5 (core/sparql->tensor' queries output))
  )
