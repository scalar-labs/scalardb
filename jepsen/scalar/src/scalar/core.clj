(ns scalar.core
  (:require [jepsen.tests :as tests]
            [jepsen.os.debian :as debian]
            [cassandra.core :as c]
            [clojurewerkz.cassaforte
             [client :as drv]
             [cql :as cql]
             [policies :refer :all]
             [query :refer :all]]
            [clojure.tools.logging :refer [debug info warn]])
  (:import (com.scalar.database.api TransactionState)
           (com.scalar.database.config DatabaseConfig)
           (com.scalar.database.storage.cassandra Cassandra)
           (com.scalar.database.service StorageModule
                                        StorageService
                                        TransactionModule
                                        TransactionService)
           (com.scalar.database.transaction.consensuscommit Coordinator)
           (com.google.inject Guice)
           (java.util Properties)))

(def RETRIES 5)

(def COORDINATOR "coordinator")
(def STATE_TABLE "state")
(def VERSION "tx_version")

(defn create-coordinator-table
  [session]
  (cql/create-keyspace session COORDINATOR
                       (if-not-exists)
                       (with {:replication
                              {"class" "SimpleStrategy"
                               "replication_factor" 3}}))
  (cql/use-keyspace session COORDINATOR)
  (cql/create-table session STATE_TABLE
                    (if-not-exists)
                    (column-definitions {:tx_id :text
                                         :tx_state :int
                                         :tx_created_at :bigint
                                         :primary-key [:tx_id]})))

(defn create-properties
  [test]
  (let [props (Properties.)]
    (.setProperty props "scalar.database.contact_points"
      (reduce #(str %1 "," %2) (:nodes test)))
    (.setProperty props "scalar.database.username" "cassandra")
    (.setProperty props "scalar.database.password" "cassandra")
    props))

(def get-properties (memoize create-properties))

(defn prepare-storage-service
  [props]
  (->
    (->> props
         DatabaseConfig.
         StorageModule.
         vector
         Guice/createInjector)
    (.getInstance StorageService)))

(def get-storage-service (memoize prepare-storage-service))

(defn prepare-storage-instance
  [props]
  (-> (DatabaseConfig. props) Cassandra.))

(defn prepare-transaction-service
  [props]
  (->
    (->> props
         DatabaseConfig.
         TransactionModule.
         vector
         Guice/createInjector)
    (.getInstance TransactionService)))

(def get-transaction-service (memoize prepare-transaction-service))

(defn start-transaction
  [props]
  (-> props get-transaction-service .start))

(defn read-coordinator
  "Return true if the status is COMMITTED. Retrun nil if the read fails."
  [coordinator id]
  (try
      (let [state (.getState coordinator id)]
        (and (.isPresent state)
             (-> state .get .getState (.equals TransactionState/COMMITTED))))
      (catch Exception e nil)))

(defn exponential-backoff
  [r]
  (Thread/sleep (* 1000 (reduce * (repeat r 2)))))

(defn read-coordinator-with-retry
  "Return true if the status is COMMITTED. When the read fails, retry to read"
  [coordinator id]
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (let [committed (read-coordinator coordinator id)]
      (if-not (nil? committed)
        committed
        (if (pos? tries)
          (recur (dec tries))
          (throw (ex-info "Failed to read a transaction state"
                          {:cause "Failed to read a transaction state"})))))))

(defn check-coordinator
  "Return the number of COMMITTED states by checking the coordinator"
  [props ids]
  (if (empty? ids)
    0
    (let [coordinator (Coordinator. (prepare-storage-instance props))]
      (->> ids
           (map #(read-coordinator-with-retry coordinator %))
           (filter true?)
           count))))

(defn scalar-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "scalar-" name)
          :os      debian/os
          :db      (c/db "3.11.3")
          :bootstrap (atom #{})
          :decommission (atom #{})
          :unknown-tx (atom #{})}
         opts))
