(ns scalar-schema.dynamo
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common]
            [scalar-schema.dynamo.auto-scaling :as scaling]
            [scalar-schema.dynamo.common :as dynamo]
            [scalar-schema.protocols :as proto])
  (:import (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model AttributeDefinition
                                                           AttributeValue
                                                           CreateTableRequest
                                                           DeleteTableRequest
                                                           GlobalSecondaryIndex
                                                           KeySchemaElement
                                                           KeyType
                                                           LocalSecondaryIndex
                                                           Projection
                                                           ProjectionType
                                                           ProvisionedThroughput
                                                           PutItemRequest
                                                           ScalarAttributeType)))

(def ^:const ^:private ^String WAIT_FOR_CREATION 10000)
(def ^:const ^:private ^String METADATA_PARTITION_KEY "table")
(def ^:const ^:private ^String PARTITION_KEY "concatenatedPartitionKey")
(def ^:const ^:private ^String CLUSTERING_KEY "concatenatedClusteringKey")
(def ^:const ^:private ^String PARTITION_KEY_COLUMN "partitionKey")
(def ^:const ^:private ^String CLUSTERING_KEY_COLUMN "clusteringKey")
(def ^:const ^:private ^String SECONDARY_INDEX_COLUMN "secondaryIndex")
(def ^:const ^:private ^String COLUMNS_COLUMN "columns")

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
      (.credentialsProvider (dynamo/get-credentials-provider user password))
      (.region (Region/of region))
      .build))

(defn- table-exists?
  [client table]
  (let [tables (-> (.listTables client) .tableNames)]
    (not (nil? (some #(= table %) tables)))))

(defn- clustering-keys-exist?
  [schema]
  (not (empty? (:clustering-key schema))))

(defn- secondary-index-exist?
  [schema]
  (not (empty? (:secondary-index schema))))

(defn- make-attribute-definition
  [name type]
  (-> (AttributeDefinition/builder)
      (.attributeName name)
      (.attributeType (type-map type))
      .build))

(defn- make-attribute-definitions
  [{:keys [clustering-key secondary-index columns] :as schema}]
  (let [key-names (into (set clustering-key) secondary-index)
        key-types (map #(columns %) key-names)
        base [(make-attribute-definition PARTITION_KEY "text")]]
    (if (clustering-keys-exist? schema)
      (-> base
          (conj (make-attribute-definition CLUSTERING_KEY "text"))
          (into (map #(make-attribute-definition %1 %2) key-names key-types)))
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

(defn- make-local-secondary-index
  [schema index-key]
  (-> (LocalSecondaryIndex/builder)
      (.indexName (dynamo/get-index-name schema index-key))
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

(defn- make-global-secondary-index
  [schema index-key ru]
  (-> (GlobalSecondaryIndex/builder)
      (.indexName (dynamo/get-global-index-name schema index-key))
      (.keySchema [(make-key-schema-element index-key KeyType/HASH)])
      (.projection (-> (Projection/builder)
                       (.projectionType (ProjectionType/ALL))
                       .build))
      (.provisionedThroughput (make-throughput ru))
      .build))

(defn- make-global-secondary-indexes
  [schema ru]
  (map #(make-global-secondary-index schema % ru) (:secondary-index schema)))

(defn- insert-metadata
  [client schema metadata-table]
  (let [table (dynamo/get-table-name schema)
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
        item (cond-> base-item
               (clustering-keys-exist? schema)
               (assoc CLUSTERING_KEY_COLUMN
                      (-> (AttributeValue/builder)
                          (.ss (:clustering-key schema)) .build))
               (secondary-index-exist? schema)
               (assoc SECONDARY_INDEX_COLUMN
                      (-> (AttributeValue/builder)
                          (.ss (:secondary-index schema)) .build)))
        request (-> (PutItemRequest/builder)
                    (.tableName metadata-table)
                    (.item item) .build)]
    (.putItem client request)))

(defn- create-metadata
  [client schema prefix]
  (let [table (dynamo/get-table-name {:database common/METADATA_DATABASE
                                      :table common/METADATA_TABLE})
        prefixed-table (if prefix (str prefix \_ table) table)
        builder (-> (CreateTableRequest/builder)
                    (.attributeDefinitions
                     [(make-attribute-definition METADATA_PARTITION_KEY
                                                 "text")])
                    (.keySchema [(make-key-schema-element
                                  METADATA_PARTITION_KEY KeyType/HASH)])
                    (.provisionedThroughput (make-throughput 1))
                    (.tableName prefixed-table))]
    (when-not (table-exists? client prefixed-table)
      (.createTable client (.build builder))
      (Thread/sleep WAIT_FOR_CREATION))
    (insert-metadata client schema prefixed-table)))

(defn- create-table
  [client schema {:keys [ru prefix] :or {ru 10}}]
  (let [table (dynamo/get-table-name schema)
        ru (if (:ru schema) (:ru schema) ru)
        builder (-> (CreateTableRequest/builder)
                    (.attributeDefinitions
                     (make-attribute-definitions schema))
                    (.keySchema (make-primary-key-schema schema))
                    (.provisionedThroughput (make-throughput ru))
                    (.tableName table))]
    (create-metadata client schema prefix)
    (if (table-exists? client table)
      (log/warn table "already exists")
      (do
        (when (clustering-keys-exist? schema)
          (.localSecondaryIndexes builder
                                  (make-local-secondary-indexes schema)))
        (when (secondary-index-exist? schema)
          (.globalSecondaryIndexes builder
                                   (make-global-secondary-indexes schema ru)))
        (.createTable client (.build builder))))))

(defn- delete-table
  [client schema]
  (let [table (dynamo/get-table-name schema)]
    (if (table-exists? client table)
      (->> (-> (DeleteTableRequest/builder) (.tableName table) .build)
           (.deleteTable client))
      (log/warn table "doesn't exist"))))

(defn make-dynamo-operator
  [{:keys [user password region no-scaling]}]
  (let [client (get-client user password region)
        scaling-client (scaling/get-scaling-client user password region)]
    (reify proto/IOperator
      (create-table [_ schema opts]
        (create-table client schema opts)
        (when (not no-scaling)
          (Thread/sleep WAIT_FOR_CREATION)
          (scaling/enable-auto-scaling scaling-client schema opts)))
      (delete-table [_ schema _]
        (scaling/disable-auto-scaling scaling-client schema)
        (delete-table client schema))
      (close [_ _]
        (.close client)
        (.close scaling-client)))))
