(ns scalar-schema.cassandra-schema
  (:require [clojure.tools.logging :as log]
            [scalar-schema.common :as common]
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
                           {:compaction {:class cs}}))))))

(defn- create-transaction-table
  [session schema opts]
  (create-table session (common/update-for-transaction schema) opts)
  (create-table session common/COORDINATOR_SCHEMA opts))

(defn- create-tables
  [session schema-file options]
  (->> (common/parse-schema schema-file)
       (map #(if (:transaction %)
               (create-transaction-table session % options)
               (create-table session % options)))
       doall))

(defn- delete-all
  [session]
  (->> (alia/execute session
                     "SELECT keyspace_name FROM system_schema.keyspaces")
       (mapcat vals)
       (filter #(not (re-find #"^system*" %)))
       (map (fn [ks] (alia/execute session (->raw (statement/drop-keyspace ks)))))
       doall))

(defn operate-cassandra
  [{:keys [schema-file host port user password]
    :or {port 9042 user "cassandra" password "cassandra"} :as options}]
  (let [cluster (get-cluster host port user password)
        session (alia/connect cluster)]
    (if (:delete-all options)
      (delete-all session)
      (create-tables session schema-file options))
    (alia/shutdown session)
    (alia/shutdown cluster)))
