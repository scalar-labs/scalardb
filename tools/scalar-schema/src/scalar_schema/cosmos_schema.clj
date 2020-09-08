(ns scalar-schema.cosmos-schema
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [scalar-schema.common :as common])
  (:import (com.azure.cosmos CosmosClient
                             CosmosClientBuilder
                             ConsistencyLevel)
           (com.azure.cosmos.models CosmosContainerProperties
                                    CosmosStoredProcedureProperties
                                    CosmosStoredProcedureRequestOptions
                                    ExcludedPath
                                    IncludedPath
                                    IndexingPolicy
                                    ThroughputProperties)
           (com.scalar.db.storage.cosmos TableMetadata)))

(def ^:const ^:private ^String METADATA_DATABASE "scalardb")
(def ^:const ^:private ^String METADATA_CONTAINER "metadata")
(def ^:const ^:private ^String METADATA_PARTITION_KEY "/id")

(def ^:const ^:private ^String
  CONTAINER_PARTITION_KEY "/concatenatedPartitionKey")
(def ^:const ^:private ^String
  PARTITION_KEY_PATH "/concatenatedPartitionKey/?")
(def ^:const ^:private ^String CLUSTERING_KEY_PATH "/clusteringKey/*")

(def ^:const ^:private REGISTERED_STORED_PROCEDURE "mutate.js")

(defn- get-client
  [uri password]
  (.buildClient (doto (CosmosClientBuilder.)
                  (.endpoint uri)
                  (.key password)
                  (.consistencyLevel ConsistencyLevel/STRONG)
                  .directMode)))

(defn- database-exists?
  [client database]
  (try
    (-> (.getDatabase client database) .read nil? not)
    (catch Exception _ false)))

(defn- container-exists?
  [client database container]
  (try
    (-> (.getDatabase client database)
        (.getContainer container)
        .read nil? not)
    (catch Exception _ false)))

(defn- make-throughput-properties
  [ru]
  (if (<= 4000 ru)
    (ThroughputProperties/createAutoscaledThroughput ru)
    (ThroughputProperties/createManualThroughput ru)))

(defn- create-database
  [client database ru]
  (.createDatabaseIfNotExists client database
                              (make-throughput-properties ru)))

(defn- update-throughput
  [client database ru]
  (let [db (.getDatabase client database)
        cur-ru (-> db .readThroughput .getMinThroughput)]
    (when (< cur-ru ru)
      (.replaceThroughput db (make-throughput-properties ru)))))

(defn- make-container-properties
  [container]
  (if (= container METADATA_CONTAINER)
    (CosmosContainerProperties. container METADATA_PARTITION_KEY)
    (let [policy (doto (IndexingPolicy.)
                   (.setIncludedPaths
                    [(IncludedPath. PARTITION_KEY_PATH)
                     (IncludedPath. CLUSTERING_KEY_PATH)])
                   (.setExcludedPaths [(ExcludedPath. "/*")]))]
      (doto (CosmosContainerProperties. container CONTAINER_PARTITION_KEY)
        (.setIndexingPolicy policy)))))

(defn- create-container
  [client database container]
  (let [prop (make-container-properties container)]
    (-> (.getDatabase client database)
        (.createContainerIfNotExists prop))))

(defn- create-metadata
  [client schema]
  (when-not (database-exists? client METADATA_DATABASE)
    (create-database client METADATA_DATABASE 400))
  (when-not (container-exists? client METADATA_DATABASE METADATA_CONTAINER)
    (create-container client METADATA_DATABASE METADATA_CONTAINER))
  (let [metadata (doto (TableMetadata.)
                   (.setId (common/get-fullname (:database schema)
                                                (:table schema)))
                   (.setPartitionKeyNames (:partition-key schema))
                   (.setClusteringKeyNames (:clustering-key schema))
                   (.setColumns (:columns schema)))]
    (-> (.getDatabase client METADATA_DATABASE)
        (.getContainer METADATA_CONTAINER)
        (.upsertItem metadata))))

(defn- register-stored-procedure
  [client database container]
  (let [scripts (-> client (.getDatabase database) (.getContainer container)
                    .getScripts)
        properties (CosmosStoredProcedureProperties.
                    REGISTERED_STORED_PROCEDURE
                    (slurp (io/resource REGISTERED_STORED_PROCEDURE)))]
    (.createStoredProcedure scripts properties
                            (CosmosStoredProcedureRequestOptions.))))

(defn- create-table
  [client schema {:keys [ru] :or {ru 400}}]
  (let [database (:database schema)
        table (:table schema)
        ru (if (:ru schema) (:ru schema) ru)]
    (create-metadata client schema)
    (if (database-exists? client database)
      (do
        (update-throughput client database ru)
        (log/warn database "already exists"))
      (create-database client database ru))
    (if (container-exists? client database table)
      (log/warn (common/get-fullname database table) "already exists")
      (do
        (create-container client database table)
        (register-stored-procedure client database table)))))

(defn- create-transaction-table
  [client schema opts]
  (create-table client (common/update-for-transaction schema) opts)
  (create-table client common/COORDINATOR_SCHEMA opts))

(defn- create-tables
  [client schema-file opts]
  (->> (common/parse-schema schema-file)
       (map #(if (:transaction %)
               (create-transaction-table client % opts)
               (create-table client % opts)))
       doall))

(defn- delete-all
  [client]
  (log/warn "Deleting all databases and tables")
  (mapv #(->> (.getId %) (.getDatabase client) .delete)
        (.readAllDatabases client)))

(defn operate-cosmos
  [{:keys [schema-file host password] :as options}]
  [host password schema-file options]
  (with-open [client (get-client host password)]
    (if (:delete-all options)
      (delete-all client)
      (create-tables client schema-file options))))
