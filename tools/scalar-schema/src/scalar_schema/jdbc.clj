(ns scalar-schema.jdbc
  (:require [clojure.string :as string]
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

(defn- get-db-spec
  [jdbc-url user password]
  {:connection-uri jdbc-url
   :user user
   :password password})

(defn- execute!
  [db-spec sql]
  (println (str "Executing " sql))
  (jdbc/execute! db-spec [sql]))

(defn- get-rdb-engine
  [jdbc-url]
  (cond
    (string/starts-with? jdbc-url "jdbc:mysql:") :mysql
    (string/starts-with? jdbc-url "jdbc:postgresql:") :postgresql
    (string/starts-with? jdbc-url "jdbc:oracle:") :oracle
    (string/starts-with? jdbc-url "jdbc:sqlserver:") :sql-server
    :else (throw (ex-info "unknown rdb engine" {}))))

(defn- enclose
  [rdb-engine name]
  (cond
    (= rdb-engine :mysql) (str "`" name "`")
    (= rdb-engine :sql-server) (str "[" name "]")
    :else (str "\"" name "\"")))

(defn- get-table-name
  ([schema table] (str schema \. table))
  ([rdb-engine schema table] (str (enclose rdb-engine schema) \. (enclose rdb-engine table))))

(defn- get-metadata-table-name
  [rdb-engine prefix]
  (let [meta-schema common/METADATA_DATABASE
        meta-table common/METADATA_TABLE]
    (get-table-name rdb-engine (if prefix (str prefix \_ meta-schema) meta-schema) meta-table)))

(defn- boolean-type
  [rdb-engine]
  (cond
    (= rdb-engine :oracle) "NUMBER(1)"
    (= rdb-engine :sql-server) "BIT"
    :else "BOOLEAN"))

(defn- boolean-value-true
  [rdb-engine]
  (cond
    (= rdb-engine :oracle) "1"
    (= rdb-engine :sql-server) "1"
    :else "true"))

(defn- boolean-value-false
  [rdb-engine]
  (cond
    (= rdb-engine :oracle) "0"
    (= rdb-engine :sql-server) "0"
    :else "false"))

(defn- key?
  [column key]
  (some #(= column %) key))

(defn- make-create-metadata-statement
  [rdb-engine prefix]
  (str "CREATE TABLE "
       (get-metadata-table-name rdb-engine prefix)
       "(" (enclose rdb-engine "full_table_name") " VARCHAR(128),"
       (enclose rdb-engine "column_name") " VARCHAR(128),"
       (enclose rdb-engine "data_type") " VARCHAR(20) NOT NULL,"
       (enclose rdb-engine "key_type") " VARCHAR(20),"
       (enclose rdb-engine "clustering_order") " VARCHAR(10),"
       (enclose rdb-engine "indexed") " " (boolean-type rdb-engine) " NOT NULL,"
       (enclose rdb-engine "index_order") " VARCHAR(10),"
       (enclose rdb-engine "ordinal_position") " INTEGER NOT NULL,"
       "PRIMARY KEY (" (enclose rdb-engine "full_table_name") ", "
       (enclose rdb-engine "column_name") "))"))

(defn- create-metadata-table
  [db-spec rdb-engine prefix]
  (try
    (execute! db-spec (make-create-metadata-statement rdb-engine prefix))
    (println "metatable table is created successfully.")
    (catch SQLException e (if (string/includes? (.getMessage e) "already") nil (throw e)))))

(defn- make-create-schema-statement
  [rdb-engine schema]
  (str "CREATE SCHEMA " (enclose rdb-engine schema)))

(defn- create-schema
  [db-spec rdb-engine schema]
  (cond
    (= rdb-engine :oracle) nil
    :else (try
            (execute! db-spec (make-create-schema-statement rdb-engine schema))
            (println "The schema" schema "is created successfully.")
            (catch SQLException e (if (or (string/includes? (.getMessage e) "already")
                                          (string/includes? (.getMessage e) "database exists"))
                                    nil (throw e))))))

(defn- create-metadata-schema
  [db-spec rdb-engine prefix]
  (let [meta-schema common/METADATA_DATABASE
        schema (if prefix (str prefix \_ meta-schema) meta-schema)]
    (create-schema db-spec rdb-engine schema)))

(defn- get-data-type
  [rdb-engine column scalardb-data-type primary-key secondary-index]
  (let [data-type (string/upper-case scalardb-data-type)
        mapping (get data-type-mapping rdb-engine)
        mapping-for-key (get data-type-mapping-for-key rdb-engine)]
    (if (key? column (concat primary-key secondary-index))
      (get mapping-for-key data-type (get mapping data-type))
      (get mapping data-type))))

(defn- make-create-table-statement
  [rdb-engine schema table partition-key clustering-key columns secondary-index]
  (let [primary-key (concat partition-key clustering-key)]
    (str "CREATE TABLE "
         (get-table-name rdb-engine schema table)
         "("
         (string/join "," (map #(let [column (enclose rdb-engine (first %))
                                      data-type (get-data-type rdb-engine (first %) (second %)
                                                               primary-key secondary-index)]
                                  (str column \  data-type)) columns))
         ", PRIMARY KEY ("
         (string/join "," (map #(enclose rdb-engine %) primary-key))
         "))")))

(defn- create-table
  [db-spec rdb-engine schema table partition-key clustering-key columns secondary-index]
  (execute! db-spec (make-create-table-statement rdb-engine schema table partition-key
                                                 clustering-key columns secondary-index)))

(defn- make-create-index-statement
  [rdb-engine schema table secondary-indexed-column]
  (let [index-name (str common/INDEX_NAME_PREFIX \_ schema \_ table \_ secondary-indexed-column)]
    (str "CREATE INDEX "
         (enclose rdb-engine index-name)
         " ON "
         (get-table-name rdb-engine schema table)
         "("
         (enclose rdb-engine secondary-indexed-column)
         ")")))

(defn- create-index
  [db-spec rdb-engine schema table secondary-index]
  (doall (map #(execute! db-spec (make-create-index-statement rdb-engine schema table %))
              secondary-index)))

(defn- make-insert-metadata-statement
  [rdb-engine prefix schema table column data-type key-type key-order secondary-indexed index-order
   ordinal-position]
  (str "INSERT INTO "
       (get-metadata-table-name rdb-engine prefix)
       " VALUES ("
       "'" (get-table-name schema table) "',"
       "'" column "',"
       "'" data-type "',"
       (if key-type (str "'" key-type "'") "NULL") ","
       (if key-order (str "'" key-order "'") "NULL") ","
       secondary-indexed ","
       (if index-order (str "'" index-order "'") "NULL") ","
       ordinal-position
       ")"))

(defn- get-key-type
  [column partition-key clustering-key]
  (cond
    (key? column partition-key) "PARTITION"
    (key? column clustering-key) "CLUSTERING"
    :else nil))

(defn- get-secondary-indexed [rdb-engine column secondary-index]
  (if (key? column secondary-index) (boolean-value-true rdb-engine)
      (boolean-value-false rdb-engine)))

(defn- zipseq
  ([coll] (partition 1 coll))
  ([c1 & colls] (let [c (cons c1 colls)] (partition (count c) (apply interleave c)))))

(defn- insert-metadata
  [db-spec rdb-engine prefix schema table partition-key clustering-key columns secondary-index]
  (doall (map #(let [column (first (first %))
                     data-type (string/upper-case (second (first %)))
                     key-type (get-key-type column partition-key clustering-key)
                     key-order (if (key? column clustering-key) "ASC" nil)
                     secondary-indexed (get-secondary-indexed rdb-engine column secondary-index)
                     index-order (if (key? column secondary-index) "ASC" nil)
                     ordinal-position (second %)]
                 (execute! db-spec (make-insert-metadata-statement rdb-engine prefix schema table
                                                                   column data-type key-type
                                                                   key-order secondary-indexed
                                                                   index-order ordinal-position)))
              (zipseq columns (iterate inc 1)))))

(defn- make-drop-table-statement
  [rdb-engine schema table]
  (str "DROP TABLE " (get-table-name rdb-engine schema table)))

(defn- drop-table
  [db-spec rdb-engine schema table]
  (execute! db-spec (make-drop-table-statement rdb-engine schema table)))

(defn- make-delete-metadata-statement
  [rdb-engine prefix schema table]
  (str "DELETE FROM "
       (get-metadata-table-name rdb-engine prefix)
       " WHERE " (enclose rdb-engine "full_table_name") " = "
       "'" (get-table-name schema table) "'"))

(defn- delete-metadata
  [db-spec rdb-engine prefix schema table]
  (execute! db-spec (make-delete-metadata-statement rdb-engine prefix schema table)))

(defn- reorder-columns
  [columns partition-key clustering-key]
  (concat (map #(vector % (get columns %)) partition-key)
          (map #(vector % (get columns %)) clustering-key)
          (filter #(not (key? (first %) (concat partition-key clustering-key)))
                  (apply list columns))))

(defn make-jdbc-operator
  [{:keys [jdbc-url user password prefix]}]
  (let [db-spec (get-db-spec jdbc-url user password)
        rdb-engine (get-rdb-engine jdbc-url)]
    (create-metadata-schema db-spec rdb-engine prefix)
    (create-metadata-table db-spec rdb-engine prefix)
    (reify proto/IOperator
      (create-table [_ {:keys [database table partition-key clustering-key columns
                               secondary-index]} _]
        (let [reordered-columns (reorder-columns columns partition-key clustering-key)
              table-name (get-table-name database table)]
          (try
            (create-schema db-spec rdb-engine database)
            (create-table db-spec rdb-engine database table partition-key clustering-key
                          reordered-columns secondary-index)
            (create-index db-spec rdb-engine database table secondary-index)
            (insert-metadata db-spec rdb-engine prefix database table partition-key clustering-key
                             reordered-columns secondary-index)
            (println table-name "is created successfully.")
            (catch SQLException e (if (string/includes? (.getMessage e) "already")
                                    (println table-name "already exists.") (throw e))))))
      (delete-table [_ {:keys [database table]} _]
        (let [table-name (get-table-name database table)]
          (try
            (delete-metadata db-spec rdb-engine prefix database table)
            (drop-table db-spec rdb-engine database table)
            (println table-name "is deleted successfully.")
            (catch SQLException e (if (or (string/includes? (.getMessage e) "Unknown table")
                                          (string/includes? (.getMessage e) "does not exist"))
                                    (println table-name "does not exist.") (throw e))))))
      (close [_ _] ()))))
