(defproject scalar "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalar"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.10-SNAPSHOT"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [clojurewerkz/cassaforte "3.0.0-alpha2-SNAPSHOT"]
                 [com.google.inject/guice "4.2.0"]
                 [com.google.guava/guava "24.1-jre"]
                 [org.yaml/snakeyaml "1.20"]]
  :resource-paths ["resources/database.jar"]
  :test-selectors {:steady :steady
                   :network :network
                   :clock :clock
                   :bootstrap :bootstrap
                   :decommission :decommission
                   :mix :mix
                   :crash :crash})
