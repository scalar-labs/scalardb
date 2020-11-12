(ns scalar-schema.operations
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common]
            [scalar-schema.cassandra :as cassandra]
            [scalar-schema.cosmos :as cosmos]
            [scalar-schema.dynamo :as dynamo]
            [scalar-schema.protocols :as proto]))

(defn- make-operator
  [{:keys [cassandra cosmos dynamo] :as opts}]
  (cond
    cassandra (cassandra/make-cassandra-operator opts)
    cosmos (cosmos/make-cosmos-operator opts)
    dynamo (dynamo/make-dynamo-operator opts)
    :else (throw (ex-info "unknown data store" {}))))

(defn create-tables
  [opts]
  (let [operator (make-operator opts)
        schema (common/parse-schema (:schema-file opts))]
    (doall (map #(proto/create-table operator % opts) schema))
    (proto/close operator opts)))

(defn delete-all
  [opts]
  (log/warn "Deleting all databases and tables in the file")
  (let [operator (make-operator opts)
        parsed (common/parse-schema (:schema-file opts))
        schema (if (or (:cosmos opts) (:dynamo opts))
                 (conj parsed {:database common/METADATA_DATABASE
                               :table common/METADATA_TABLE})
                 parsed)]
    (doall (map #(proto/delete-table operator % opts) schema))
    (proto/close operator opts)))
