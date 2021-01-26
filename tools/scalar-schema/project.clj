(defproject scalar-schema "2.4.1"
  :description "Schema tool for Scalar DB"
  :url "http://github.com/scalar-labs/scalardb"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.cli "1.0.194"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.apache.logging.log4j/log4j-core "2.13.3"]
                 [org.slf4j/slf4j-log4j12 "1.7.30"]
                 [cheshire "5.10.0"]
                 [com.azure/azure-cosmos "4.8.0"]
                 [software.amazon.awssdk/dynamodb "2.14.24"]
                 [software.amazon.awssdk/applicationautoscaling "2.14.24"]
                 [com.google.guava/guava "24.1-jre"]
                 [cc.qbits/alia "4.3.3"]
                 [cc.qbits/hayt "4.1.0"]]
  :resource-paths ["resources"
                   "stored_procedure"]
  :java-source-paths ["java/src"]
  :repl-options {:init-ns scalar-schema.cassandra-schema}
  :main scalar-schema.core
  :profiles {:uberjar {:aot :all
                       :uberjar-name "scalar-schema-standalone-%s.jar"
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
