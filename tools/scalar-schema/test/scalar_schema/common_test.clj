(ns scalar-schema.common-test
  (:require [clojure.test :refer :all]
            [scalar-schema.common :as common]))

(deftest get-fullname-test
  (is (= "ks.tbl" (common/get-fullname "ks" "tbl"))))

(deftest parse-schema
  (let [schema (common/parse-schema {:schema-file
                                     "sample_schema/sample_schema.json"})
        table0 (first (filter #(and (= "sample_db" (:database %))
                                    (= "sample_table" (:table %))) schema))
        table1 (first (filter #(and (= "sample_db" (:database %))
                                    (= "sample_table1" (:table %))) schema))
        table2 (first (filter #(and (= "sample_db" (:database %))
                                    (= "sample_table2" (:table %))) schema))
        coordinator (first (filter #(= "coordinator" (:database %)) schema))]
    ;; sample_db.sample_table
    (is (= ["c1"] (:partition-key table0)))
    (is (= ["c4"] (:clustering-key table0)))
    (is (nil? (:transaction table0)))
    (is (= {"c1" "int"
            "c2" "text"
            "c3" "blob"
            "c4" "int"
            "c5" "boolean"}
           (:columns table0)))
    (is (= 5000 (:ru table0)))
    (is (= "LCS" (:compaction-strategy table0)))

    ;; sample_db.sample_table1
    (is (= ["c1"] (:partition-key table1)))
    (is (= ["c4"] (:clustering-key table1)))
    (is (true? (:transaction table1)))
    (is (= {"c1" "int"
            "c2" "text"
            "c3" "int"
            "c4" "int"
            "c5" "boolean"
            "before_c2" "text"
            "before_c3" "int"
            "before_c5" "boolean"
            "before_tx_committed_at" "bigint"
            "before_tx_id" "text"
            "before_tx_prepared_at" "bigint"
            "before_tx_state" "int"
            "before_tx_version" "int"
            "tx_committed_at" "bigint"
            "tx_id" "text"
            "tx_prepared_at" "bigint"
            "tx_state" "int"
            "tx_version" "int"}
           (:columns table1)))

    ;; sample_db.sample_table2
    (is (= ["c1"] (:partition-key table2)))
    (is (= ["c4" "c3"] (:clustering-key table2)))
    (is (false? (:transaction table2)))
    (is (= {"c1" "int"
            "c2" "text"
            "c3" "int"
            "c4" "int"
            "c5" "boolean"}
           (:columns table2)))

    ;; coordinator.state
    (is (= ["tx_id"] (:partition-key coordinator)))
    (is (empty? (:clustering-key coordinator)))
    (is (nil? (:transaction coordinator)))
    (is (= {"tx_id" "text"
            "tx_state" "int"
            "tx_created_at" "bigint"}
           (:columns coordinator)))))
