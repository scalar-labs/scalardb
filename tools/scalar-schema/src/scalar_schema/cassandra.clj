(ns scalar-schema.cassandra
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common]
            [scalar-schema.protocols :as proto]
            [qbits.alia :as alia]
            [qbits.hayt :refer [->raw]]
            [qbits.hayt.dsl.clause :as clause]
            [qbits.hayt.dsl.statement :as statement]))

(def ^:private compaction-strategies
  {"STCS" :SizeTieredCompactionStrategy
   "LCS" :LeveledCompactionStrategy
   "TWCS" :TimeWindowCompactionStrategy})

(defn- get-cluster
  [host port user password]
  (alia/cluster {:contact-points [host]
                 :port port
                 :jmx-reporting? false
                 :credentials {:user user
                               :password password}}))

(defn- make-replication-param
  [{:keys [network-strategy replication-factor]
    :or {network-strategy "SimpleStrategy"}}]
  (condp = network-strategy
    "SimpleStrategy" {"class" "SimpleStrategy"
                      "replication_factor" replication-factor}
    "NetworkTopologyStrategy" {"class" "NetworkTopologyStrategy"
                               "dc1" replication-factor} ;; TODO: other DCs
    (throw (ex-info "Unknown topology strategy is specified."
                    {:network-strategy network-strategy}))))

(defn- create-database
  [session database options]
  (alia/execute session (statement/create-keyspace
                         (keyword database)
                         (clause/if-exists false)
                         (clause/with
                          {:replication (make-replication-param options)}))))

(defn- key-set->vec
  [key-set]
  (reduce (fn [v k] (conj v (keyword k))) [] key-set))

(defn- make-key-array
  [schema]
  (into (conj [] (key-set->vec (:partition-key schema)))
        (key-set->vec (:clustering-key schema))))

(defn- make-column-definitions
  [schema]
  (-> (reduce-kv (fn [m c t] (assoc m (keyword c) (keyword t)))
                 {} (:columns schema))
      (assoc :primary-key (make-key-array schema))))

(defn create-table
  [session schema options]
  (let [keyspace (keyword (:database schema))
        table (keyword (:table schema))
        cs (compaction-strategies (:compaction-strategy schema "STCS"))]
    (create-database session (:database schema) options)
    (alia/execute session (->raw (statement/use-keyspace keyspace)))
    (alia/execute session
                  (->raw (statement/create-table
                          table
                          (clause/if-exists false)
                          (clause/column-definitions
                           (make-column-definitions schema))
                          (clause/with
                           {:compaction {:class cs}}))))
    (doseq [index (:secondary-index schema)]
      (alia/execute session
                    (->raw (statement/create-index
                             table
                             index
                             (clause/index-name (str (name table) \_
                                                     common/INDEX_NAME_PREFIX
                                                     \_ index))
                             (clause/if-exists false)))))))

(defn- delete-table
  [session schema]
  (alia/execute session
                (->raw (statement/drop-keyspace (:database schema)
                                                (clause/if-exists true)))))

(defn make-cassandra-operator
  [{:keys [host port user password]
    :or {port 9042 user "cassandra" password "cassandra"}}]
  (let [cluster (get-cluster host port user password)
        session (alia/connect cluster)]
    (reify proto/IOperator
      (create-table [_ schema opts]
        (create-table session schema opts))
      (delete-table [_ schema _]
        (delete-table session schema))
      (close [_ _]
        (alia/shutdown session)
        (alia/shutdown cluster)))))
