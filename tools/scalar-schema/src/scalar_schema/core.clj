(ns scalar-schema.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [scalar-schema.operations :as op])
  (:gen-class))

(def cli-options
  [[nil "--cassandra" "Operate for Cassandra"]
   [nil "--cosmos" "Operate for Cosmos DB"]
   [nil "--dynamo" "Operate for DynamoDB"]
   [nil "--jdbc" "Operate for a JDBC database"]
   ["-h" "--host DB_HOST" "Address of Cassandra like IP or URI address of your Cosmos DB account"]
   ["-P" "--port DB_PORT" "Port of Cassandra. This option is ignored when Cosmos DB and DynamoDB."
    :parse-fn #(Integer/parseInt %)]
   ["-u" "--user USERNAME" "Username of the database. This option is ignored when Cosmos DB and DynamoDB."]
   ["-p" "--password PASSWORD" "Password of Cassandra or Cosmos DB account or JDBC database"]
   ["-f" "--schema-file SCHEMA_JSON" "Schema file"]
   ["-r" "--ru RESOURCE_UNIT" "Base RU for each table on Cosmos DB or DynamoDB. The RU of the coordinator for Scalar DB transaction is specified by this option. This option is ignored when Cassandra."
    :parse-fn #(Integer/parseInt %)]
   ["-R" "--replication-factor REPLICATION_FACTOR"
    "The number of replicas. This options is ignored when Cosmos DB and DynamoDB."
    :default 1 :parse-fn #(Integer/parseInt %)]
   ["-n" "--network-strategy NETWORK_STRATEGY"
    "The network topology strategy. SimpleStrategy or NetworkTopologyStrategy. This options is ignored when Cosmos DB and DynamoDB."]
   ["-D" "--delete-all" "All database will be deleted, if this is enabled."]
   [nil "--region REGION" "Region where the tool creates tables for DynamoDB"]
   [nil "--prefix NAMESPACE_PREFIX" "Namespace prefix. The prefix is added to all the namespaces."]
   [nil "--no-scaling" "Disable auto-scaling"]
   ["-j" "--jdbc-url JDBC_URL" "JDBC URL for a JDBC database"]
   [nil "--help"]])

(defn -main [& args]
  (let [{:keys [options summary errors] {:keys [help]} :options}
        (parse-opts args cli-options)]
    (cond
      help (println summary)
      errors (do
               (println (str "ERROR: " errors))
               (println summary)
               (System/exit 1))
      (:delete-all options) (op/delete-all options)
      :else (op/create-tables options))))
