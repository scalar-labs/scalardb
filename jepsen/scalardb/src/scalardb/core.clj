(ns scalardb.core
  (:require [jepsen.tests :as tests]
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

(def ^:const RETRIES 8)
(def ^:const RETRIES_FOR_RECONNECTION 3)

(def ^:const COORDINATOR "coordinator")
(def ^:const STATE_TABLE "state")
(def ^:const VERSION "tx_version")

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn create-coordinator-table!
  [session test]
  (cql/create-keyspace session COORDINATOR
                       (if-not-exists)
                       (with {:replication
                              {"class" "SimpleStrategy"
                               "replication_factor" (:rf test)}}))
  (cql/use-keyspace session COORDINATOR)
  (cql/create-table session STATE_TABLE
                    (if-not-exists)
                    (column-definitions {:tx_id :text
                                         :tx_state :int
                                         :tx_created_at :bigint
                                         :primary-key [:tx_id]})))

(defn- create-properties
  [nodes]
  (let [props (Properties.)]
    (if (= (count nodes) 1)
      (.setProperty props
                    "scalar.database.contact_points"
                    (first nodes))
      (.setProperty props
                    "scalar.database.contact_points"
                    (reduce #(str %1 "," %2) nodes)))
    (.setProperty props "scalar.database.username" "cassandra")
    (.setProperty props "scalar.database.password" "cassandra")
    props))

(defn- close-storage!
  [test]
  (let [storage (:storage test)]
    (locking storage
      (when-not (nil? @storage)
        (.close @storage)
        (reset! storage nil)
        (info "The current storage service closed")))))

(defn- close-transaction!
  [test]
  (let [transaction (:transaction test)]
    (locking transaction
      (when-not (nil? @transaction)
        (.close @transaction)
        (reset! transaction nil)
        (info "The current transaction service closed")))))

(defn close-all!
  [test]
  (close-storage! test)
  (close-transaction! test))

(defn prepare-storage-service!
  [test]
  (close-storage! test)
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (if-not (pos? tries)
      (warn "Failed to connect to the cluster")
      (if-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 StorageModule.
                                 vector
                                 Guice/createInjector)]
        (try
          (->> (.getInstance injector StorageService)
               (reset! (:storage test)))
          (catch Exception e
            (warn (.getMessage e))))
        (when-not (nil? (:storage test))
          (recur (dec tries)))))))

(defn prepare-transaction-service!
  [test]
  (close-transaction! test)
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (if-not (pos? tries)
      (warn "Failed to connect to the cluster")
      (if-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 TransactionModule.
                                 vector
                                 Guice/createInjector)]
        (try
          (->> (.getInstance injector TransactionService)
               (reset! (:transaction test)))
          (catch Exception e
            (warn (.getMessage e))))
        (when-not (nil? (:transaction test))
          (recur (dec tries)))))))

(defn start-transaction
  [test]
  (some-> test :transaction deref .start))

(defn- is-committed-state?
  "Return true if the status is COMMITTED. Retrun nil if the read fails."
  [coordinator id]
  (try
      (let [state (.getState coordinator id)]
        (and (.isPresent state)
             (-> state .get .getState (.equals TransactionState/COMMITTED))))
      (catch Exception e nil)))

(defn check-transaction-states
  "Return the number of COMMITTED states by checking the coordinator. Return nil if it fails."
  [test ids]
  (loop [tries RETRIES]
    (if-not (pos? tries)
      nil
      (do
        (when (< tries RETRIES)
          (exponential-backoff (- RETRIES tries)))
        (when (zero? (mod tries RETRIES_FOR_RECONNECTION))
          (prepare-storage-service! test)) ; reconnection
        (let [coordinator (Coordinator. (deref (:storage test)))
              committed   (map (partial is-committed-state? coordinator) ids)]
          (if-not (some nil? committed)
            (->> committed (filter true?) count)
            (recur (dec tries))))))))

(defn scalardb-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "scalardb-" name)
          :storage (atom nil)
          :transaction (atom nil)}
         opts))
