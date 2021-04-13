(ns scalar-schema.operations
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common]
            [scalar-schema.cassandra :as cassandra]
            [scalar-schema.cosmos :as cosmos]
            [scalar-schema.dynamo :as dynamo]
            [scalar-schema.jdbc :as jdbc]
            [scalar-schema.protocols :as proto]))

(defn- make-operator
  [{:keys [cassandra cosmos dynamo jdbc] :as opts}]
  (cond
    cassandra (cassandra/make-cassandra-operator opts)
    cosmos (cosmos/make-cosmos-operator opts)
    dynamo (dynamo/make-dynamo-operator opts)
    jdbc (jdbc/make-jdbc-operator opts)
    :else (throw (ex-info "unknown data store" {}))))

(defn create-tables
  [opts]
  (let [operator (make-operator opts)
        schema (common/parse-schema opts)]
    (doall (map #(proto/create-table operator % opts) schema))
    (proto/close operator opts)))

(defn delete-all
  [{:keys [cosmos dynamo jdbc prefix] :as opts}]
  (log/warn "Deleting all databases and tables in the file")
  (let [operator (make-operator opts)
        parsed (common/parse-schema opts)
        schema (if (or cosmos dynamo jdbc)
                 (conj parsed
                       {:database (if prefix
                                    (str prefix \_ common/METADATA_DATABASE)
                                    common/METADATA_DATABASE)
                        :table common/METADATA_TABLE})
                 parsed)]
    (doall (map #(proto/delete-table operator % opts) schema))
    (proto/close operator opts)))
