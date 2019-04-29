(ns scalardb.transfer_append
  (:require [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
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
  (:import (com.scalar.database.api Put
                                    Scan
                                    Scan$Ordering
                                    Scan$Ordering$Order
                                    Result)
           (com.scalar.database.io IntValue
                                   Key)
           (com.scalar.database.exception.transaction CrudException
                                                      UnknownTransactionStatusException)))

(def ^:const KEYSPACE "jepsen")
(def ^:const TABLE "transfer")
(def ^:const ACCOUNT_ID "account_id")
(def ^:const BALANCE "balance")
(def ^:const AGE "age")

(def ^:const INITIAL_BALANCE 10000)
(def ^:const NUM_ACCOUNTS 10)
(def ^:const total-balance (* NUM_ACCOUNTS INITIAL_BALANCE))

(def ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(defn- create-transfer-table!
  [session test]
  (cql/create-keyspace session KEYSPACE
                       (if-not-exists)
                       (with {:replication {"class" "SimpleStrategy"
                                            "replication_factor" (:rf test)}}))
  (cql/use-keyspace session KEYSPACE)
  (cql/create-table session TABLE
                    (if-not-exists)
                    (column-definitions {:account_id             :int
                                         :age                    :int
                                         :balance                :int
                                         :tx_id                  :text
                                         :tx_prepared_at         :bigint
                                         :tx_committed_at        :bigint
                                         :tx_state               :int
                                         :tx_version             :int
                                         :before_balance         :int
                                         :before_tx_committed_at :bigint
                                         :before_tx_id           :text
                                         :before_tx_prepared_at  :bigint
                                         :before_tx_state        :int
                                         :before_tx_version      :int
                                         :primary-key            [:account_id :age]}))
  (cql/alter-table session TABLE
                   (with {:compaction-options (c/compaction-strategy)})))

(defn prepare-scan
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Scan.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withOrdering (Scan$Ordering. "age" Scan$Ordering$Order/DESC))))

(defn prepare-scan-with-limit
  [id limit]
  (-> id
      (prepare-scan)
      (.withLimit limit)))

(defn scan-and-return-first-result
  [tx scan]
  (first (.scan tx scan)))

(defn prepare-put
  [id age balance]
  (-> (Put. (Key. [(IntValue. ACCOUNT_ID id)]) (Key. [(IntValue. AGE age)]))
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue (IntValue. BALANCE balance))))

(defn populate-accounts
  "Insert initial records with transaction.
  This method assumes that n is small (< 100)"
  [test n balance]
  (let [tx (scalar/start-transaction test)]
    (try
      (dotimes [i n]
        (->>
          (prepare-put i 0 balance)
          (.put tx)))
      (.commit tx)
      (catch Exception e
        (throw (RuntimeException. (.getMessage e)))))))

(defn calc-new-balance
  "Calculate the new value from the result of transaction.get()"
  [^Result result amount]
  (-> result (.getValue BALANCE) .get .get (+ amount)))

(defn calc-new-age
  "Calculate the new value from the result of transaction.get()"
  [^Result result]
  (-> result (.getValue AGE) .get .get inc))

(defn tx-transfer
  [tx from to amount]
  (let [^Result fromResult (scan-and-return-first-result tx (prepare-scan-with-limit from 1))
        ^Result toResult (scan-and-return-first-result tx (prepare-scan-with-limit to 1))]
    (->> (prepare-put from (calc-new-age fromResult) (calc-new-balance fromResult (- amount)))
         (.put tx))
    (->> (prepare-put to (calc-new-age toResult) (calc-new-balance toResult amount))
         (.put tx))
    (.commit tx)))

(defn read-record
  "Read a record with a transaction. If read fails, this function returns nil."
  [tx id]
  (try
    (scan-and-return-first-result tx (prepare-scan-with-limit id 1))
    (catch CrudException _ nil)))

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

(defn get-balance-from-result
  "Get the balance from a result"
  [^Result result]
  (-> result (.getValue BALANCE) .get .get))

(defn get-age-from-result
  "Get the age from a result"
  [^Result result]
  (-> result (.getValue AGE) .get .get))

(defn get-balances-and-ages
  "Read all records with a transaction. Return the balances and ages."
  [test n]
  (let [results (read-all-with-retry test n)]
    (map #(assoc {} :balance (get-balance-from-result %)
                    :age (get-age-from-result %))
         results)))

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
                      (assoc op :type :fail, :error {:unknown-tx-status (.getId tx)}))
                    (catch Exception e
                      (try-reconnection! test)
                      (assoc op :type :fail, :error (.getMessage e))))
                  (do
                    (try-reconnection! test)
                    (assoc op :type :fail, :error "Skipped due to no connection")))
      :get-all  (if-let [result (get-balances-and-ages test (:num op))]
                  (assoc op :type, :ok :value result)
                  (assoc op :type, :fail, :error "Failed to get balances and ages"))
      :get-num-records (if-let [result (let [tx (scalar/start-transaction test)]
                                         (->> (range (:num op))
                                              (map prepare-scan)
                                              (map #(.scan tx %))
                                              (map count)
                                              (reduce +)))]
                          (assoc op :type :ok, :value result)
                          (assoc op :type :fail, :error "Failed to get number of records"))
      :check-tx (let [unknown (:unknown-tx test)]
                  (if-let [num-committed (if (empty? @unknown)
                                           0
                                           (scalar/check-transaction-states test @unknown))]
                    (assoc op :type :ok, :value num-committed)
                    (assoc op :type :fail, :error "Failed to check status")))))

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
             :amount (+ 1 (rand-int 1000))}}))

(def diff-transfer
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

(defn get-all
  [test _]
  {:type :invoke
   :f :get-all
   :num (-> test :model :num)})

(defn get-num-records
  [test _]
  {:type :invoke
   :f :get-num-records
   :num (-> test :model :num)})

(defn check-tx
  [test _]
  {:type :invoke
   :f :check-tx})

(defn consistency-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [read-result (->> history
                             (r/filter op/ok?)
                             (r/filter #(= :get-all (:f %)))
                             (into [])
                             last
                             :value)
            actual-balance (->> read-result
                                (map :balance)
                                (reduce +))
            bad-balance (if-not (= actual-balance total-balance)
                          {:type     :wrong-balance
                           :expected total-balance
                           :actual   actual-balance})
            actual-age (->> read-result
                            (map :age)
                            (reduce +))
            expected-age (let [num-records (->> history
                                                (r/filter #(= :get-num-records (:f %)))
                                                (into [])
                                                last
                                                :value)]
                           (- num-records (:num (:model test)))) ; age starts at 0 so should be num-records minus num-accounts
            bad-age (if-not (= actual-age expected-age)
                      {:type :wrong-age
                       :expected expected-age
                       :actual actual-age})
            checked-committed (->> history
                                   (r/filter #(= :check-tx (:f %)))
                                   (into [])
                                   last
                                   ((fn [x]
                                      (if (= (:type x) :ok) (:value x) 0))))]
        {:valid? (and (empty? bad-balance) (empty? bad-age))
         :total-balance actual-balance
         :total-age actual-age
         :committed-unknown-tx checked-committed
         :bad-balance bad-balance
         :bad-age bad-age}))))

(defn transfer-append-test
  [opts]
  (merge (scalar/scalardb-test (str "transfer-append-" (:suffix opts))
                               {:client     (TransferClient. (atom false) NUM_ACCOUNTS INITIAL_BALANCE)
                                :model      {:num NUM_ACCOUNTS}
                                :unknown-tx (atom #{})
                                :failures   (atom 0)
                                :generator  (gen/phases
                                              (->> [diff-transfer]
                                                   (conductors/std-gen opts))
                                              (conductors/terminate-nemesis opts)
                                              (gen/clients (gen/once check-tx))
                                              (gen/clients (gen/once get-all))
                                              (gen/clients (gen/once get-num-records)))
                                :checker    (consistency-checker)})
         opts))
