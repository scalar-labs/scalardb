(ns scalar-schema.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [scalar-schema.cassandra-schema :as cassandra-schema]
            [scalar-schema.cosmos-schema :as cosmos-schema]
            [scalar-schema.dynamo-schema :as dynamo-schema])
  (:gen-class))

(def cli-options
  [[nil "--cassandra" "Operate for Cassandra"]
   [nil "--cosmos" "Operate for Cosmos DB"]
   [nil "--dynamo" "Operate for DynamoDB"]
   ["-h" "--host DB_HOST" "Address of Cassandra like IP or URI address of your Cosmos DB account"]
   ["-P" "--port DB_PORT" "Port of Cassandra. This option is ignored when Cosmos DB and DynamoDB."
    :parse-fn #(Integer/parseInt %)]
   ["-u" "--user USERNAME" "Username of the database. This option is ignored when Cosmos DB and DynamoDB."]
   ["-p" "--password ACCOUNT_PASSWORD" "Password of Cassandra or Cosmos DB account"]
   ["-f" "--schema-file SCHEMA_JSON" "Schema file"]
   ["-r" "--ru RESOURCE_UNIT" "Base RU for each table on Cosmos DB. The RU of the coordinator for Scalar DB transaction is specified by this option. This option is ignored when Cassandra."
    :parse-fn #(Integer/parseInt %)]
   ["-R" "--replication-factor REPLICATION_FACTOR"
    "The number of replicas. This options is ignored when Cosmos DB and DynamoDB."
    :default 1 :parse-fn #(Integer/parseInt %)]
   ["-n" "--network-strategy NETWORK_STRATEGY"
    "The network topology strategy. SimpleStrategy or NetworkTopologyStrategy. This options is ignored when Cosmos DB and DynamoDB."]
   ["-D" "--delete-all" "All database will be deleted, if this is enabled."]
   [nil "--help"]])

(defn -main [& args]
  (let [{:keys [options summary errors]
         {:keys [cassandra cosmos dynamo help]} :options}
        (parse-opts args cli-options)]
    (if (or help errors)
      (do (when (not help)
            (println (str "ERROR: " errors)))
          (println summary))
      (do
        (when cassandra (cassandra-schema/operate-cassandra options))
        (when cosmos (cosmos-schema/operate-cosmos options))
        (when dynamo (dynamo-schema/operate-dynamo options))))))
