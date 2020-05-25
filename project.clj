(defproject metabase/impala "1.0.0"
  :min-lein-version "2.5.0"

  :dependencies
  [[org.clojure/core.logic "0.8.11"
    :exclusions [org.clojure/clojure]]]

  :jvm-opts
  ["-XX:+IgnoreUnrecognizedVMOptions"
   "--add-modules=java.xml.bind"]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.0"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :omit-source   true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "impala.metabase-driver.jar"}})
