(ns scalardb.core
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

(def ^:const RETRIES 5)
(def ^:const RETRIES_FOR_RECONNECTION 3)

(def ^:const COORDINATOR "coordinator")
(def ^:const STATE_TABLE "state")
(def ^:const VERSION "tx_version")

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

(defn prepare-storage-service!
  [test]
  (let [storage (:storage test)]
    (when-not (nil? @storage)
      (.close @storage)
      (info "reconnecting to the cluster"))
    (when-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 StorageModule.
                                 vector
                                 Guice/createInjector)]
      (reset! storage (.getInstance injector StorageService)))))

(defn prepare-transaction-service!
  [test]
  (let [transaction (:transaction test)]
    (when-not (nil? @transaction)
      (.close @transaction)
      (info "reconnecting to the cluster"))
    (when-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 TransactionModule.
                                 vector
                                 Guice/createInjector)]
      (reset! transaction (.getInstance injector TransactionService)))))

(defn start-transaction
  [test]
  (-> test :transaction deref .start))

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
  [test id]
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (when (zero? (mod tries RETRIES_FOR_RECONNECTION))
      (prepare-storage-service! test)) ; reconnection
    (let [coordinator (Coordinator. (deref (:storage test)))
          committed (read-coordinator coordinator id)]
      (if-not (nil? committed)
        committed
        (if (pos? tries)
          (recur (dec tries))
          (throw (ex-info "Failed to read a transaction state"
                          {:cause "Failed to read a transaction state"})))))))

(defn check-coordinator
  "Return the number of COMMITTED states by checking the coordinator"
  [test ids]
  (if (empty? ids)
    0
    (let [coordinator (Coordinator. (deref (:storage test)))]
      (->> ids
           (map #(read-coordinator-with-retry test %))
           (filter true?)
           count))))

(defn close-all!
  [test]
  (let [storage (:storage test)
        transaction (:transaction test)]
    (when-not (nil? @storage)
      (.close @storage)
      (compare-and-set! storage @storage nil))
    (when-not (nil? @transaction)
      (.close @transaction)
      (compare-and-set! transaction @transaction nil))))

(defn scalardb-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "scalardb-" name)
          :os      debian/os
          :storage (atom nil)
          :transaction (atom nil)}
         opts))
