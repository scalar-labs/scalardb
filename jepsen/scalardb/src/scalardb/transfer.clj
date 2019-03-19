(ns scalardb.transfer
  (:require [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [nemesis :as nemesis]
             [tests  :as tests]]
            [jepsen.checker.timeline :as timeline]
            [cassandra
             [core :as c]
             [conductors :as conductors]]
            [clojurewerkz.cassaforte
             [client :as drv]
             [cql :as cql]
             [policies :refer :all]
             [query :refer :all]]
            [clojure.tools.logging :refer [debug info warn]]
            [clojure.core.reducers :as r]
            [knossos.op :as op]
            [scalardb.core :as scalar])
  (:import (com.scalar.database.api Consistency
                                    DistributedStorage
                                    DistributedTransaction
                                    Get
                                    Isolation
                                    Put
                                    Result)
           (com.scalar.database.io IntValue
                                   Key)
           (com.scalar.database.exception.transaction CommitException
                                                      CrudException
                                                      UnknownTransactionStatusException)))

(def ^:const KEYSPACE "jepsen")
(def ^:const TABLE "transfer")
(def ^:const ACCOUNT_ID "account_id")
(def ^:const BALANCE "balance")

(def ^:const INITIAL_BALANCE 1000)
(def ^:const NUM_ACCOUNTS 10)
(def total-balance (* NUM_ACCOUNTS INITIAL_BALANCE))

(def ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(defn- create-transfer-table!
  [session test]
  (cql/create-keyspace session KEYSPACE
                       (if-not-exists)
                       (with {:replication
                              {"class" "SimpleStrategy"
                               "replication_factor" (:rf test)}}))
  (cql/use-keyspace session KEYSPACE)
  (cql/create-table session TABLE
                    (if-not-exists)
                    (column-definitions {:account_id :int
                                         :balance :int
                                         :tx_id :text
                                         :tx_version :int
                                         :tx_state :int
                                         :tx_prepared_at :bigint
                                         :tx_committed_at :bigint
                                         :before_account_id :int
                                         :before_balance :int
                                         :before_tx_id :text
                                         :before_tx_version :int
                                         :before_tx_state :int
                                         :before_tx_prepared_at :bigint
                                         :before_tx_committed_at :bigint
                                         :primary-key [:account_id]}))
  (cql/alter-table session TABLE
                   (with {:compaction-options (c/compaction-strategy)})))

(defn prepare-get
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withConsistency Consistency/LINEARIZABLE)))

(defn prepare-put
  [id v]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue v)
      (.withConsistency Consistency/LINEARIZABLE)))

(defn populate-accounts
  "Insert initial records with transaction.
  This method assumes that n is small (< 100)"
  [test n balance]
  (let [tx (scalar/start-transaction test)]
    (try
      (dotimes [i n]
        (->>
          (prepare-put i (IntValue. BALANCE balance))
          (.put tx)))
      (.commit tx)
      (catch Exception e
        (throw (RuntimeException. (.getMessage e)))))))

(defn calc-new-val
  "Calculate the new value from the result of transaction.get()"
  [r amount]
  (IntValue. BALANCE
             (-> r .get (.getValue BALANCE) .get .get (+ amount))))

(defn tx-transfer
  [tx from to amount]
  (let [fromResult (.get tx (prepare-get from))
        toResult (.get tx (prepare-get to))]
    (->> (calc-new-val fromResult (- amount))
         (prepare-put from)
         (.put tx))
    (->> (calc-new-val toResult amount)
         (prepare-put to)
         (.put tx))
    (.commit tx)))

(defn read-record
  "Read a record with a transaction. If read fails, this function returns nil."
  [tx i]
  (try
    (.get tx (prepare-get i))
    (catch CrudException e nil)))

(defn read-all-records
  "Read records from 0 .. (n - 1)"
  [test n]
  (let [tx (scalar/start-transaction test)]
    (map #(read-record tx %) (range n))))

(defn read-all-with-retry
  "Read records from 0 .. (n - 1) and retry if needed"
  [test n]
  (loop [tries scalar/RETRIES]
    (when (< tries scalar/RETRIES)
      (scalar/exponential-backoff (- scalar/RETRIES tries)))
    (when (zero? (mod tries scalar/RETRIES_FOR_RECONNECTION))
      (scalar/prepare-transaction-service! test))
    (let [results (read-all-records test n)]
      (if (empty? (filter nil? results))
        results
        (if (pos? tries)
          (recur (dec tries))
          (throw (ex-info "Failed to read all records"
                          {:cause "Failed to read all records"})))))))

(defn get-balance
  "Get a balance from a result"
  [r]
  (-> r .get (.getValue BALANCE) .get .get))

(defn get-version
  "Get a version from a result"
  [r]
  (-> r .get (.getValue scalar/VERSION) .get .get))

(defn get-balances-and-versions
  "Read all records with a transaction. Return only balances and versions."
  [test n]
  (let [results (read-all-with-retry test n)]
    (map #(assoc {} :balance (get-balance %) :version (get-version %)) results)))

(defn- try-reconnection!
  [test]
  (when (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (scalar/prepare-transaction-service! test)
    (reset! (:failures test) 0)))

(defrecord TransferClient [initialized? n initial-balance]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (let [session (drv/connect (->> test :nodes (map name)))]
          (create-transfer-table! session test)
          (scalar/create-coordinator-table! session test)
          (drv/disconnect! session))
        (scalar/prepare-storage-service! test)
        (scalar/prepare-transaction-service! test)
        (populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (if-let [tx (scalar/start-transaction test)]
                  (try
                    (tx-transfer tx (-> op :value :from) (-> op :value :to) (-> op :value :amount))
                    (assoc op :type :ok)
                    (catch UnknownTransactionStatusException e
                      (swap! (:unknown-tx test) conj (.getId tx))
                      (assoc op :type :fail :error {:unknown-tx-status (.getId tx)}))
                    (catch Exception e
                      (try-reconnection! test)
                      (assoc op :type :fail :error (.getMessage e))))
                  (do
                    (try-reconnection! test)
                    (assoc op :type :fail :error "Skipped due to no connection")))
      :get-all  (if-let [results (get-balances-and-versions test (:num op))]
                  (assoc op :type :ok :value results)
                  (assoc op :type :fail :error "Failed to get balances"))
      :check-tx (let [unknown (:unknown-tx test)]
                  (if-let [num-committed (if (empty? @unknown)
                                           0
                                           (scalar/check-transaction-states test @unknown))]
                    (assoc op :type :ok :value num-committed)
                    (assoc op :type :fail :error "Failed to check status")))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn transfer
  [test _]
  (let [n (-> test :model :num)]
    {:type :invoke
     :f :transfer
     :value {:from (rand-int n)
             :to (rand-int n)
             :amount (+ 1 (rand-int 5))}}))

(def diff-transfer
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

(defn get-all
  [test _]
  {:type :invoke
   :f :get-all
   :num (-> test :model :num)})

(defn check-tx
  [test _]
  {:type :invoke
   :f :check-tx})

(defn consistency-checker
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [read-result (->> history
                             (r/filter op/ok?)
                             (r/filter #(= :get-all (:f %)))
                             (r/filter identity)
                             (into [])
                             last
                             :value)
            actual-balance (->> read-result
                                (map :balance)
                                (reduce +))
            bad-balance (if-not (= actual-balance total-balance)
                          {:type :wrong-balance
                           :expected total-balance
                           :actual actual-balance})
            actual-version (->> read-result
                                (map :version)
                                (reduce +))
            checked-committed (->> history
                                   (r/filter #(= :check-tx (:f %)))
                                   (r/filter identity)
                                   (into [])
                                   last
                                   ((fn [x]
                                      (if (= (:type x) :ok) (:value x) 0))))
            total-ok (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :transfer (:f %)))
                          (r/filter identity)
                          (into [])
                          count
                          (+ checked-committed))
            expected-version (-> total-ok
                                 (* 2)                       ; update 2 records per a transfer
                                 (+ (-> test :model :num)))  ; initial insertions
            bad-version (if-not (= actual-version expected-version)
                          {:type :wrong-version
                           :expected expected-version
                           :actual actual-version})]
        {:valid? (and (empty? bad-balance) (empty? bad-version))
         :total-version actual-version
         :committed-unknown-tx checked-committed
         :bad-balance bad-balance
         :bad-version bad-version}))))

(defn transfer-test
  [opts]
  (merge (scalar/scalardb-test (str "transfer-" (:suffix opts))
                               {:client (TransferClient. (atom false) NUM_ACCOUNTS INITIAL_BALANCE)
                                :model  {:num NUM_ACCOUNTS}
                                :unknown-tx (atom #{})
                                :failures (atom 0)
                                :generator (gen/phases
                                            (->> [diff-transfer]
                                                 (conductors/std-gen opts))
                                            (gen/clients (gen/once check-tx))
                                            (gen/clients (gen/once get-all)))
                                :checker (checker/compose
                                           {:perf    (checker/perf)
                                            :timeline (timeline/html)
                                            :details (consistency-checker)})})
         opts))
