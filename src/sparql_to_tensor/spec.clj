(ns sparql-to-tensor.spec
  (:require [clojure.spec.alpha :as s]
            [sparclj.core :as sparql])
  (:import (java.io File)))

; ----- CLI parameters -----

(s/def ::help? true?)

(s/def ::symmetric? true?)

(s/def ::output (s/and (partial instance? File)
                       (memfn isDirectory)))

(s/def ::queries (s/coll-of string?))

(s/def ::params (s/keys :req [::sparql/url ::output]
                        :opt [::sparql/page-size ::help? ::symmetric?]))
