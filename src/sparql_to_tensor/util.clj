(ns sparql-to-tensor.util
  (:require [clojure.string :as string]))

(defn- exit
  "Exit with `status` and message `msg`.
  `status` 0 is OK, `status` 1 indicates error."
  [^Integer status
   ^String msg]
  {:pre [(#{0 1} status)]}
  (binding [*out* (if (zero? status) *out* *err*)]
    (println msg))
  (System/exit status))

(def die
  (partial exit 1))

(def info
  (partial exit 0))

(defn ->integer
  [s]
  (Integer/parseInt s))

(defn file-exists?
  "Test if file at `path` exists and is a file."
  [path]
  (and (.exists path) (.isFile path)))

(def join-lines
  (partial string/join \newline))
