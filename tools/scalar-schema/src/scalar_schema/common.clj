(ns scalar-schema.common
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as cheshire]))

(def ^:const ^String METADATA_DATABASE "scalardb")
(def ^:const ^String METADATA_TABLE "metadata")

(def COORDINATOR_SCHEMA {:database "coordinator"
                         :table "state"
                         :partition-key #{"tx_id"}
                         :clustering-key #{}
                         :columns {"tx_id" "text"
                                   "tx_state" "int"
                                   "tx_created_at" "bigint"}})

(def TRANSACTION_METADATA_COLUMNS {"tx_committed_at" "bigint"
                                   "tx_id" "text"
                                   "tx_prepared_at" "bigint"
                                   "tx_state" "int"
                                   "tx_version" "int"})

(defn get-fullname
  [database table]
  (str database "." table))

(defn- format-schema
  [schema]
  (map (fn [[k v]]
         (let [[db tbl] (str/split (name k) #"\.")
               v' (merge v {:columns (reduce-kv (fn [m c t]
                                                  (assoc m
                                                         (name c)
                                                         (.toLowerCase t)))
                                                {} (:columns v))})]
           (assoc v' :database db :table tbl)))
       schema))

(defn- make-transaction-columns
  [schema]
  (let [s (merge (:columns schema) TRANSACTION_METADATA_COLUMNS)]
    (->> (reduce (fn [m [name type]]
                   (if (or (contains? (:partition-key schema) name)
                           (contains? (:clustering-key schema) name))
                     m
                     (assoc m (str "before_" name) type)))
                 {} s)
         (merge s))))

(defn- apply-transaction
  [schema]
  (map (fn [table-schema]
         (if (:transaction table-schema)
           (merge table-schema
                  {:columns (make-transaction-columns table-schema)})
           table-schema))
       schema))

(defn- add-coordinator
  [schema]
  (if (some #(:transaction %) schema)
    (merge schema COORDINATOR_SCHEMA)
    schema))

(defn parse-schema
  [schema-file]
  (->> (cheshire/parse-stream (io/reader schema-file) true
                              #(when (or (= % "partition-key")
                                         (= % "clustering-key")) #{}))
       format-schema
       apply-transaction
       add-coordinator))
