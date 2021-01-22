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
(def ^:const ^:private ^String SECONDARY_INDEX_PATH "/values/")

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
  [client {:keys [database table]}]
  (try
    (-> (.getDatabase client database)
        (.getContainer table)
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
  [{:keys [table secondary-index]}]
  (if (= table common/METADATA_TABLE)
    (CosmosContainerProperties. table METADATA_PARTITION_KEY)
    (let [policy (doto (IndexingPolicy.)
                   (.setIncludedPaths
                    (into [(IncludedPath. PARTITION_KEY_PATH)
                           (IncludedPath. CLUSTERING_KEY_PATH)]
                          (map #(IncludedPath.
                                 (str SECONDARY_INDEX_PATH % "/?"))
                               secondary-index)))
                   (.setExcludedPaths [(ExcludedPath. "/*")]))]
      (doto (CosmosContainerProperties. table CONTAINER_PARTITION_KEY)
        (.setIndexingPolicy policy)))))

(defn- create-container
  [client {:keys [database] :as schema}]
  (let [prop (make-container-properties schema)]
    (-> (.getDatabase client database)
        (.createContainerIfNotExists prop))))

(defn- create-metadata
  [client
   {:keys [database table partition-key clustering-key secondary-index columns]}
   prefix]
  (let [prefixed-database (if prefix
                            (str prefix \_ common/METADATA_DATABASE)
                            common/METADATA_DATABASE)
        metadata (doto (CosmosTableMetadata.)
                   (.setId (common/get-fullname database table))
                   (.setPartitionKeyNames (set partition-key))
                   (.setClusteringKeyNames (set clustering-key))
                   (.setSecondaryIndexNames (set secondary-index))
                   (.setColumns columns))]
    (when-not (database-exists? client prefixed-database)
      (create-database client prefixed-database 400 true))
    (when-not (container-exists? client {:database prefixed-database
                                         :table common/METADATA_TABLE})
      (create-container client {:database prefixed-database
                                :table common/METADATA_TABLE}))
    (-> (.getDatabase client prefixed-database)
        (.getContainer common/METADATA_TABLE)
        (.upsertItem metadata))))

(defn- register-stored-procedure
  [client {:keys [database table]}]
  (let [scripts (-> client (.getDatabase database) (.getContainer table)
                    .getScripts)
        properties (CosmosStoredProcedureProperties.
                    REGISTERED_STORED_PROCEDURE
                    (slurp (io/resource REGISTERED_STORED_PROCEDURE)))]
    (.createStoredProcedure scripts properties
                            (CosmosStoredProcedureRequestOptions.))))

(defn- create-table
  [client {:keys [database table] :as schema}
   {:keys [ru prefix no-scaling] :or {ru 400 no-scaling false}}]
  (let [ru (if (:ru schema) (:ru schema) ru)]
    (create-metadata client schema prefix)
    (if (database-exists? client database)
      (do
        (update-throughput client database ru no-scaling)
        (log/warn database "already exists"))
      (create-database client database ru no-scaling))
    (if (container-exists? client schema)
      (log/warn (common/get-fullname database table) "already exists")
      (do
        (create-container client schema)
        (register-stored-procedure client schema)))))

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
