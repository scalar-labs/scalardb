(ns scalar-schema.jdbc
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [scalar-schema.common :as common]
            [scalar-schema.protocols :as proto]
            [clojure.java.jdbc :as jdbc])
  (:import (java.sql SQLException)))

(def ^:private data-type-mapping
  {:mysql      {"INT"     "INT"
                "BIGINT"  "BIGINT"
                "TEXT"    "LONGTEXT"
                "FLOAT"   "FLOAT"
                "DOUBLE"  "DOUBLE"
                "BOOLEAN" "BOOLEAN"
                "BLOB"    "LONGBLOB"}
   :postgresql {"INT"     "INT"
                "BIGINT"  "BIGINT"
                "TEXT"    "TEXT"
                "FLOAT"   "FLOAT"
                "DOUBLE"  "DOUBLE PRECISION"
                "BOOLEAN" "BOOLEAN"
                "BLOB"    "BYTEA"}
   :oracle     {"INT"     "INT"
                "BIGINT"  "NUMBER(19)"
                "TEXT"    "VARCHAR(4000)"
                "FLOAT"   "BINARY_FLOAT"
                "DOUBLE"  "BINARY_DOUBLE"
                "BOOLEAN" "NUMBER(1)"
                "BLOB"    "BLOB"}
   :sql-server {"INT"     "INT"
                "BIGINT"  "BIGINT"
                "TEXT"    "VARCHAR(8000)"
                "FLOAT"   "FLOAT(24)"
                "DOUBLE"  "FLOAT"
                "BOOLEAN" "BIT"
                "BLOB"    "VARBINARY(8000)"}})

(def ^:private data-type-mapping-for-key
  {:mysql      {"TEXT"    "VARCHAR(256)"
                "BLOB"    "VARBINARY(256)"}
   :postgresql {"TEXT"    "VARCHAR(10485760)"}
   :oracle     {"BLOB"    "RAW(2000)"}
   :sql-server {}})

(defn- key?
  [column key]
  (if key (.contains key column) false))

(defn- get-table-name
  ([database table] (str database \. table))
  ([database table {:keys [enclosure-fn]}] (str (enclosure-fn database) \. (enclosure-fn table))))

(defn- get-metadata-table-name
  [{:keys [prefix] :as opts}]
  (let [database common/METADATA_DATABASE
        table common/METADATA_TABLE]
    (get-table-name (if prefix (str prefix \_ database) database) table opts)))

(defn- get-metadata-schema
  [{:keys [boolean-type]}]
  {"full_table_name" "VARCHAR(128)"
   "column_name"     "VARCHAR(128)"
   "data_type" "VARCHAR(20) NOT NULL"
   "key_type" "VARCHAR(20)"
   "clustering_order" "VARCHAR(10)"
   "indexed" (str boolean-type \  "NOT NULL")
   "index_order" "VARCHAR(10)"
   "ordinal_position" "INTEGER NOT NULL"})

(defn- make-create-metadata-statement
  [{:keys [enclosure-fn] :as opts}]
  (str "CREATE TABLE "
       (get-metadata-table-name opts)
       \(
       (->> (map #(str (enclosure-fn (key %)) \  (val %))
                 (get-metadata-schema opts))
            (string/join \,))
       ", PRIMARY KEY ("
       (->> (map enclosure-fn ["full_table_name" "column_name"])
            (string/join \,))
       "))"))

(defn- create-metadata-table
  [{:keys [execution-fn] :as opts}]
  (try
    (execution-fn (make-create-metadata-statement opts))
    (log/info "metatable table is created successfully.")
    (catch SQLException e
      (when-not (string/includes? (.getMessage e) "already")
        (throw e)))))

(defn- make-create-database-statement
  [database {:keys [enclosure-fn]}]
  (str "CREATE SCHEMA " (enclosure-fn database)))

(defn- create-database
  [database {:keys [rdb-engine execution-fn] :as opts}]
  (when-not (= rdb-engine :oracle)
    (try
      (execution-fn (make-create-database-statement database opts))
      (log/info "The database" database "is created successfully.")
      (catch SQLException e
        (when-not (or (string/includes? (.getMessage e) "already")
                      (string/includes? (.getMessage e) "database exists"))
          (throw e))))))

(defn- create-metadata-database
  [{:keys [prefix] :as opts}]
  (let [meta-database common/METADATA_DATABASE
        database (if prefix (str prefix \_ meta-database) meta-database)]
    (create-database database opts)))

(defn- make-create-table-statement
  [{:keys [database table partition-key clustering-key reordered-columns]}
   {:keys [enclosure-fn data-type-fn] :as opts}]
  (let [primary-key (concat partition-key clustering-key)]
    (str "CREATE TABLE "
         (get-table-name database table opts)
         \(
         (->> (map #(let [column (enclosure-fn (first %))
                          data-type (data-type-fn (first %) (second %))]
                      (str column \  data-type)) reordered-columns)
              (string/join \,))
         ", PRIMARY KEY ("
         (->> (map enclosure-fn primary-key)
              (string/join \,))
         "))")))

(defn- create-table
  [schema {:keys [execution-fn] :as opts}]
  (execution-fn (make-create-table-statement schema opts)))

(defn- make-create-index-statement
  [{:keys [database table]} {:keys [enclosure-fn] :as opts} indexed-column]
  (let [index-name (str common/INDEX_NAME_PREFIX \_ database \_ table \_ indexed-column)]
    (str "CREATE INDEX "
         (enclosure-fn index-name)
         " ON "
         (get-table-name database table opts)
         \(
         (enclosure-fn indexed-column)
         \))))

(defn- create-index
  [{:keys [secondary-index] :as schema} {:keys [execution-fn] :as opts}]
  (doall (map #(execution-fn (make-create-index-statement schema opts %)) secondary-index)))

(defn- get-key-type
  [column partition-key clustering-key]
  (cond
    (key? column partition-key) "PARTITION"
    (key? column clustering-key) "CLUSTERING"
    :else nil))

(defn- secondary-indexed? [column secondary-index]
  (key? column secondary-index))

(defn- make-insert-metadata-statement
  [{:keys [database table partition-key clustering-key secondary-index]}
   {:keys [boolean-value-fn] :as opts}
   column data-type ordinal-position]
  (let [key-type (get-key-type column partition-key clustering-key)
        key-order (if (key? column clustering-key) "'ASC'" "NULL")
        indexed (boolean-value-fn (secondary-indexed? column secondary-index))
        index-order (if (key? column secondary-index) "'ASC'" "NULL")]
    (str "INSERT INTO "
         (get-metadata-table-name opts)
         " VALUES ("
         "'" (get-table-name database table) "',"
         "'" column "',"
         "'" data-type "',"
         (if key-type (str "'" key-type "'") "NULL") ","
         key-order ","
         indexed ","
         index-order ","
         ordinal-position
         ")")))

(defn- insert-metadata
  [{:keys [reordered-columns] :as schema}
   {:keys [execution-fn] :as opts}]
  (loop [[[column data-type] & rest] reordered-columns pos 1]
    (when column
      (execution-fn
       (make-insert-metadata-statement schema opts column (string/upper-case data-type) pos))
      (recur rest (inc pos)))))

(defn- make-drop-table-statement
  [{:keys [database table]} opts]
  (str "DROP TABLE " (get-table-name database table opts)))

(defn- drop-table
  [schema {:keys [execution-fn] :as opts}]
  (execution-fn (make-drop-table-statement schema opts)))

(defn- make-delete-metadata-statement
  [{:keys [database table]} {:keys [enclosure-fn] :as opts}]
  (str "DELETE FROM "
       (get-metadata-table-name opts)
       " WHERE " (enclosure-fn "full_table_name") " = "
       "'" (get-table-name database table) "'"))

(defn- delete-metadata
  [schema {:keys [execution-fn] :as opts}]
  (execution-fn (make-delete-metadata-statement schema opts)))

(defn- reorder-columns
  [{:keys [columns partition-key clustering-key]}]
  (concat (map #(vector % (get columns %)) partition-key)
          (map #(vector % (get columns %)) clustering-key)
          (filter #(not (key? (first %) (concat partition-key clustering-key)))
                  (apply list columns))))

(defn- get-execution-fn
  [{:keys [jdbc-url user password]}]
  (fn [statement]
    (log/debug "Executing" statement)
    (-> {:connection-uri jdbc-url :user user :password password}
        (jdbc/execute! [statement]))))

(defn- get-rdb-engine
  [{:keys [jdbc-url]}]
  (condp #(string/starts-with? %2 %1)  jdbc-url
    "jdbc:mysql:" :mysql
    "jdbc:postgresql:" :postgresql
    "jdbc:oracle:" :oracle
    "jdbc:sqlserver:" :sql-server
    (throw (ex-info "unknown rdb engine" {}))))

(defn- get-enclosure-fn
  [rdb-engine]
  #(cond
     (= rdb-engine :mysql) (str "`" % "`")
     (= rdb-engine :sql-server) (str "[" % "]")
     :else (str "\"" % "\"")))

(defn- get-boolean-type
  [rdb-engine]
  (get (get data-type-mapping rdb-engine) "BOOLEAN"))

(defn- get-boolean-value-fn
  [rdb-engine]
  #(if (or (= rdb-engine :oracle) (= rdb-engine :sql-server))
     (if % "1" "0") (if % "true" "false")))

(defn- get-data-type-fn
  [rdb-engine {:keys [partition-key clustering-key secondary-index]}]
  #(let [data-type (string/upper-case %2)
         mapping (get data-type-mapping rdb-engine)
         mapping-for-key (get data-type-mapping-for-key rdb-engine)]
     (if (key? %1 (concat partition-key clustering-key secondary-index))
       (get mapping-for-key data-type (get mapping data-type))
       (get mapping data-type))))

(defn make-jdbc-operator
  [opts]
  (let [rdb-engine (get-rdb-engine opts)
        opts (assoc opts
                    :execution-fn (get-execution-fn opts)
                    :rdb-engine rdb-engine
                    :enclosure-fn (get-enclosure-fn rdb-engine)
                    :boolean-type (get-boolean-type rdb-engine)
                    :boolean-value-fn (get-boolean-value-fn rdb-engine))]
    (reify proto/IOperator
      (create-table [_ {:keys [database table] :as schema} _]
        (let [schema (assoc schema :reordered-columns (reorder-columns schema))
              opts (assoc opts :data-type-fn (get-data-type-fn rdb-engine schema))
              table-name (get-table-name database table)]
          (try
            (create-metadata-database opts)
            (create-metadata-table opts)
            (create-database database opts)
            (create-table schema opts)
            (create-index schema opts)
            (insert-metadata schema opts)
            (log/info table-name "is created successfully.")
            (catch SQLException e (if (string/includes? (.getMessage e) "already")
                                    (log/warn table-name "already exists.") (throw e))))))
      (delete-table [_ {:keys [database table] :as schema} _]
        (let [table-name (get-table-name database table)]
          (try
            (create-metadata-database opts)
            (create-metadata-table opts)
            (delete-metadata schema opts)
            (drop-table schema opts)
            (log/info table-name "is deleted successfully.")
            (catch SQLException e (if (or (string/includes? (.getMessage e) "Unknown table")
                                          (string/includes? (.getMessage e) "does not exist"))
                                    (log/warn table-name "does not exist.") (throw e))))))
      (close [_ _] ()))))
