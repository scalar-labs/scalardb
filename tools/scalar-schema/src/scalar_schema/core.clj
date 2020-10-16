(ns scalar-schema.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [scalar-schema.cassandra-schema :as cassandra-schema]
            [scalar-schema.cosmos-schema :as cosmos-schema])
  (:gen-class))

(def cli-options
  [[nil "--cassandra" "Operate for Cassandra"]
   [nil "--cosmos" "Operate for Cosmos DB"]
   ["-h" "--host DB_HOST" "Address of Cassandra like IP or URI address of your Cosmos DB account"]
   ["-P" "--port DB_PORT" "Port of Cassandra. This option is ignored when Cosmos DB."
    :parse-fn #(Integer/parseInt %)]
   ["-u" "--user USERNAME" "Username of the database. This option is ignored when Cosmos DB."]
   ["-p" "--password ACCOUNT_PASSWORD" "Password of Cassandra or Cosmos DB account"]
   ["-f" "--schema-file SCHEMA_JSON" "Schema file"]
   ["-r" "--ru RESOURCE_UNIT" "Base RU for each table on Cosmos DB. The RU of the coordinator for Scalar DB transaction is specified by this option. This option is ignored when Cassandra."
    :default 400 :parse-fn #(Integer/parseInt %)]
   ["-R" "--replication-factor REPLICATION_FACTOR"
    "The number of replicas. This options is ignored when Cosmos DB."
    :default 1 :parse-fn #(Integer/parseInt %)]
   ["-n" "--network-strategy NETWORK_STRATEGY"
    "The network topology strategy. SimpleStrategy or NetworkTopologyStrategy. This options is ignored when Cosmos DB."]
   ["-D" "--delete-all" "All database will be deleted, if this is enabled."]
   [nil "--help"]])

(defn -main [& args]
  (let [{:keys [options summary errors]
         {:keys [cassandra cosmos help]} :options}
        (parse-opts args cli-options)]
    (if (or help errors)
      (do (when (not help)
            (println (str "ERROR: " errors)))
          (println summary))
      (do
        (when cassandra (cassandra-schema/operate-cassandra options))
        (when cosmos (cosmos-schema/operate-cosmos options))))))


