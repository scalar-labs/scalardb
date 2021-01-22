(ns scalar-schema.common
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as cheshire]))

(def ^:const ^String METADATA_DATABASE "scalardb")
(def ^:const ^String METADATA_TABLE "metadata")
(def ^:const ^String INDEX_NAME_PREFIX "index")

(def COORDINATOR_SCHEMA {:database "coordinator"
                         :table "state"
                         :partition-key ["tx_id"]
                         :clustering-key []
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

(defn- filter-secondary-index
  [schema]
  (map (fn [table-schema]
         (->> (filter #(not (.contains (:partition-key table-schema) %))
                      (:secondary-index table-schema))
              (assoc table-schema :secondary-index)))
       schema))

(defn- make-transaction-columns
  [schema]
  (let [s (merge (:columns schema) TRANSACTION_METADATA_COLUMNS)]
    (->> (reduce (fn [m [name type]]
                   (if (or (some #(= name %) (:partition-key schema))
                           (some #(= name %) (:clustering-key schema)))
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

(defn- add-prefix
  [schema prefix]
  (if prefix
    (map (fn [table-schema]
           (assoc table-schema
                  :database
                  (str prefix \_ (:database table-schema))))
         schema)
    schema))

(defn parse-schema
  [{:keys [schema-file prefix]}]
  (-> (cheshire/parse-stream (io/reader schema-file) true
                             #(when (or (= % "partition-key")
                                        (= % "clustering-key")
                                        (= % "secondary-index")) []))
      format-schema
      filter-secondary-index
      apply-transaction
      add-coordinator
      (add-prefix prefix)))
