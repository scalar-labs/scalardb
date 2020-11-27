(ns scalar-schema.cosmos
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [scalar-schema.common :as common]
            [scalar-schema.protocols :as proto])
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
           (com.scalar.db.storage.cosmos CosmosTableMetadata)))

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
  [ru no-scaling]
  (if (or (<= ru 4000) no-scaling)
    (ThroughputProperties/createManualThroughput ru)
    (ThroughputProperties/createAutoscaledThroughput ru)))

(defn- create-database
  [client database ru no-scaling]
  (.createDatabaseIfNotExists client database
                              (make-throughput-properties ru no-scaling)))

(defn- update-throughput
  [client database ru no-scaling]
  (let [db (.getDatabase client database)
        cur-ru (-> db .readThroughput .getMinThroughput)]
    (when (< cur-ru ru)
      (.replaceThroughput db (make-throughput-properties ru no-scaling)))))

(defn- make-container-properties
  [container]
  (if (= container common/METADATA_TABLE)
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
  (when-not (database-exists? client common/METADATA_DATABASE)
    (create-database client common/METADATA_DATABASE 400 true))
  (when-not (container-exists? client common/METADATA_DATABASE common/METADATA_TABLE)
    (create-container client common/METADATA_DATABASE common/METADATA_TABLE))
  (let [metadata (doto (CosmosTableMetadata.)
                   (.setId (common/get-fullname (:database schema)
                                                (:table schema)))
                   (.setPartitionKeyNames (set (:partition-key schema)))
                   (.setClusteringKeyNames (set (:clustering-key schema)))
                   (.setColumns (:columns schema)))]
    (-> (.getDatabase client common/METADATA_DATABASE)
        (.getContainer common/METADATA_TABLE)
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
  [client schema {:keys [ru no-scaling] :or {ru 400 no-scaling false}}]
  (let [database (:database schema)
        table (:table schema)
        ru (if (:ru schema) (:ru schema) ru)]
    (create-metadata client schema)
    (if (database-exists? client database)
      (do
        (update-throughput client database ru no-scaling)
        (log/warn database "already exists"))
      (create-database client database ru no-scaling))
    (if (container-exists? client database table)
      (log/warn (common/get-fullname database table) "already exists")
      (do
        (create-container client database table)
        (register-stored-procedure client database table)))))

(defn- delete-table
  [client {:keys [database]}]
  (when (database-exists? client database)
    (->> (.getDatabase client database) .delete)))

(defn make-cosmos-operator
  [{:keys [host password]}]
  (let [client (get-client host password)]
    (reify proto/IOperator
      (create-table [_ schema opts]
        (create-table client schema opts))
      (delete-table [_ schema _]
        (delete-table client schema))
      (close [_ _]
        (.close client)))))
