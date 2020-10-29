(ns scalar-schema.dynamo-schema
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [scalar-schema.common :as common])
  (:import (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    StaticCredentialsProvider)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model AttributeDefinition
                                                           AttributeValue
                                                           CreateTableRequest
                                                           DeleteTableRequest
                                                           KeySchemaElement
                                                           KeyType
                                                           LocalSecondaryIndex
                                                           Projection
                                                           ProjectionType
                                                           ProvisionedThroughput
                                                           PutItemRequest
                                                           ScalarAttributeType)))

(def ^:const ^:private ^String WAIT_FOR_CREATION 10000)
(def ^:const ^:private ^String METADATA_DATABASE "scalardb")
(def ^:const ^:private ^String METADATA_TABLE "metadata")
(def ^:const ^:private ^String METADATA_PARTITION_KEY "table")
(def ^:const ^:private ^String PARTITION_KEY "concatenatedPartitionKey")
(def ^:const ^:private ^String CLUSTERING_KEY "concatenatedClusteringKey")
(def ^:const ^:private ^String INDEX_NAME_PREFIX "index")
(def ^:const ^:private ^String PARTITION_KEY_COLUMN "partitionKey")
(def ^:const ^:private ^String CLUSTERING_KEY_COLUMN "clusteringKey")
(def ^:const ^:private ^String COLUMNS_COLUMN "columns")
(def ^:private META_TABLE (common/get-fullname METADATA_DATABASE
                                               METADATA_TABLE))

(def ^:private type-map
  {"int" ScalarAttributeType/N
   "bigint" ScalarAttributeType/N
   "float" ScalarAttributeType/N
   "double" ScalarAttributeType/N
   "text" ScalarAttributeType/S
   "blob" ScalarAttributeType/B})

(defn- get-client
  [user password region]
  (-> (DynamoDbClient/builder)
      (.credentialsProvider (StaticCredentialsProvider/create
                             (AwsBasicCredentials/create user password)))
      (.region (Region/of region))
      .build))

(defn- table-exists?
  [client table]
  (let [tables (-> (.listTables client) .tableNames)]
    (not (nil? (some #(= table %) tables)))))

(defn- clustering-keys-exist?
  [schema]
  (not (empty? (:clustering-key schema))))

(defn- make-attribute-definition
  [name type]
  (-> (AttributeDefinition/builder)
      (.attributeName name)
      (.attributeType (type-map type))
      .build))

(defn- make-attribute-definitions
  [schema]
  (let [clustering-keys (:clustering-key schema)
        clustering-key-types (map #((:columns schema) %) clustering-keys)
        base [(make-attribute-definition PARTITION_KEY "text")]]
    (if (clustering-keys-exist? schema)
      (-> base
          (conj (make-attribute-definition CLUSTERING_KEY "text"))
          (into (mapv #(make-attribute-definition %1 %2)
                      clustering-keys clustering-key-types)))
      base)))

(defn- make-key-schema-element
  [name key-type]
  (-> (KeySchemaElement/builder)
      (.attributeName name)
      (.keyType key-type)
      .build))

(defn- make-primary-key-schema
  [schema]
  (if (clustering-keys-exist? schema)
    [(make-key-schema-element PARTITION_KEY KeyType/HASH)
     (make-key-schema-element CLUSTERING_KEY KeyType/RANGE)]
    [(make-key-schema-element PARTITION_KEY KeyType/HASH)]))

(defn- get-index-name
  [schema key-name]
  (str (common/get-fullname (:database schema) (:table schema))
       "." INDEX_NAME_PREFIX "." key-name))

(defn- make-local-secondary-index
  [schema index-key]
  (-> (LocalSecondaryIndex/builder)
      (.indexName (get-index-name schema index-key))
      (.keySchema [(make-key-schema-element PARTITION_KEY KeyType/HASH)
                   (make-key-schema-element index-key KeyType/RANGE)])
      (.projection (-> (Projection/builder)
                       (.projectionType (ProjectionType/ALL))
                       .build))
      .build))

(defn- make-local-secondary-indexes
  [schema]
  (map #(make-local-secondary-index schema %) (:clustering-key schema)))

(defn- make-throughput
  [ru]
  (-> (ProvisionedThroughput/builder)
      (.readCapacityUnits (long ru))
      (.writeCapacityUnits (long ru))
      .build))

(defn- insert-metadata
  [client schema]
  (let [table (common/get-fullname (:database schema) (:table schema))
        columns (reduce-kv (fn [m c t]
                             (assoc m c (-> (AttributeValue/builder)
                                            (.s t) .build)))
                           {} (:columns schema))
        base-item {METADATA_PARTITION_KEY (-> (AttributeValue/builder)
                                              (.s table) .build)
                   PARTITION_KEY_COLUMN (-> (AttributeValue/builder)
                                            (.ss (:partition-key schema))
                                            .build)
                   COLUMNS_COLUMN (-> (AttributeValue/builder)
                                      (.m columns) .build)}
        item (if (clustering-keys-exist? schema)
               (assoc base-item CLUSTERING_KEY_COLUMN
                      (-> (AttributeValue/builder)
                          (.ss (:clustering-key schema)) .build))
               base-item)
        request (-> (PutItemRequest/builder)
                    (.tableName META_TABLE)
                    (.item item) .build)]
    (.putItem client request)))

(defn- create-metadata
  [client schema]
  (let [builder (-> (CreateTableRequest/builder)
                    (.attributeDefinitions
                     [(make-attribute-definition METADATA_PARTITION_KEY
                                                 "text")])
                    (.keySchema [(make-key-schema-element
                                  METADATA_PARTITION_KEY KeyType/HASH)])
                    (.provisionedThroughput (make-throughput 1))
                    (.tableName META_TABLE))]
    (when-not (table-exists? client META_TABLE)
      (.createTable client (.build builder))
      (Thread/sleep WAIT_FOR_CREATION))
    (insert-metadata client schema)))

(defn- create-table
  [client schema {:keys [ru] :or {ru 10}}]
  (let [table (common/get-fullname (:database schema) (:table schema))
        ru (if (:ru schema) (:ru schema) ru)
        builder (-> (CreateTableRequest/builder)
                    (.attributeDefinitions
                     (make-attribute-definitions schema))
                    (.keySchema (make-primary-key-schema schema))
                    (.provisionedThroughput (make-throughput ru))
                    (.tableName table))]
    (create-metadata client schema)
    (if (table-exists? client table)
      (log/warn table "already exists")
      (do
        (when (clustering-keys-exist? schema)
          (.localSecondaryIndexes builder
                                  (make-local-secondary-indexes schema)))
        (.createTable client (.build builder))))))

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

(defn- delete-table
  [client table]
  (if (table-exists? client table)
    (->> (-> (DeleteTableRequest/builder) (.tableName table) .build)
         (.deleteTable client))
    (log/warn table "doesn't exist")))

(defn- delete-all
  [client schema-file]
  (log/warn "Deleting all databases and tables in the file")
  (let [tables (map #(common/get-fullname (:database %) (:table %))
                    (common/parse-schema schema-file))
        coordinator (common/get-fullname (:database common/COORDINATOR_SCHEMA)
                                         (:table common/COORDINATOR_SCHEMA))]
    (doall (map #(delete-table client %)
                (into tables [META_TABLE coordinator])))))

(defn operate-dynamo
  [{:keys [schema-file host user password region] :as options}]
  (with-open [client (get-client user password region)]
    (if (:delete-all options)
      (delete-all client schema-file)
      (create-tables client schema-file options))))
