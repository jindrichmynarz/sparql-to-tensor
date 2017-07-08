(ns sparql-to-tensor.cli
  (:gen-class)
  (:require [sparql-to-tensor.spec :as spec]
            [sparql-to-tensor.util :as util]
            [sparql-to-tensor.core :as core]
            [sparclj.core :as sparql]
            [sparclj.spec :as sparql-spec]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.spec.alpha :as s]
            [mount.core :as mount]
            [slingshot.slingshot :refer [try+]]))

; ----- Private functions -----

(defn- error-msg
  [errors]
  (util/join-lines (cons "The following errors occurred while parsing your command:\n" errors)))

(defn- usage
  [summary]
  (util/join-lines ["Export RDF from SPARQL endpoints as tensors"
                    ""
                    "Usage: sparql_to_tensor [parameters]"
                    ""
                    "Parameters:"
                    summary]))

(defn- validate-params
  [params]
  (when-not (s/valid? ::spec/params params)
    (util/die (str "The provided arguments are invalid.\n\n"
                   (s/explain-str ::spec/params params)))))

(defn- main
  [{::sparql/keys [url]
    ::spec/keys [output queries]
    :as params}]
  (validate-params params)
  (try+ (mount/start-with-args params)
        (catch [:type ::sparql/endpoint-not-found] _
          (util/die (format "SPARQL endpoint <%s> was not found." url))))
  (core/sparql->tensor output queries)
  (shutdown-agents))

; ----- Private vars -----

(def ^:private cli-options
  [["-e" "--endpoint ENDPOINT" "SPARQL endpoint's URL"
    :id ::sparql/url
    :validate [(every-pred sparql-spec/http? sparql-spec/valid-url?)
               "The endpoint must be a valid absolute HTTP(S) URL."]] 
   ["-q" "--query QUERY" "SPARQL query to retrieve relations. Can be repeated."
    :id ::spec/queries
    :assoc-fn (fn [m k query]
                (update m k (partial into query)))
    :parse-fn (comp vector slurp io/as-file)]
   ["-p" "--page-size PAGE_SIZE" "Number of results to fetch in one request"
    :id ::sparql/page-size
    :parse-fn util/->integer
    :validate [pos? "Number of results must be a positive number."]
    :default 10000]
   ["-o" "--output OUTPUT" "Directory to output tensor slices"
    :id ::spec/output
    :parse-fn (fn [dirname]
                (let [dir (io/as-file dirname)]
                  (when-not (.exists dir) (.mkdir dir))
                  dir))]
   ["-h" "--help" "Display help information"
    :id ::spec/help?]])

; ----- Public functions -----

(defn -main
  [& args]
  (let [{{::spec/keys [help?]
          :as params} :options
         :keys [errors summary]} (parse-opts args cli-options)]
    (cond help? (util/info (usage summary))
          errors (util/die (error-msg errors))
          :else (main params))))
