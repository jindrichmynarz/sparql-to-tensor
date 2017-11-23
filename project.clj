(defproject sparql-to-tensor "0.1.0"
  :description "Export RDF from SPARQL endpoints to tensors"
  :url "https://github.com/jindrichmynarz/sparql-to-tensor"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [org.clojure/tools.cli "0.3.5"]
                 [sparclj "0.1.8"]
                 [mount "0.1.11"]
                 [slingshot "0.12.2"]
                 [com.taoensso/timbre "4.10.0"]
                 [stencil "0.5.0"]
                 [org.la4j/la4j "0.6.0"]
                 [org.clojure/data.csv "0.1.4"]
                 [org.clojure/data.avl "0.0.17"]]
  :main sparql-to-tensor.cli
  :profiles {:dev {:plugins [[lein-binplus "0.4.2"]]}
             :uberjar {:aot :all
                       :uberjar-name "sparql_to_tensor.jar"}}
  :bin {:name "sparql_to_tensor"})
