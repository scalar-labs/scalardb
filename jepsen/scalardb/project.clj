(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalardb"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.10-SNAPSHOT"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [clojurewerkz/cassaforte "3.0.0-alpha2-SNAPSHOT"]
                 [com.scalar-labs/scalardb "1.0.0" :exclusions [org.slf4j/slf4j-log4j12]]]
  :main scalardb.runner
  :aot [scalardb.runner])
