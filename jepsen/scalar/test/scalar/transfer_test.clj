(ns scalar.transfer-test
  (:require [clojure.test :refer :all]
            [scalar.transfer :refer :all]
            [jepsen [core :as jepsen]]))

(defn run-test!
  [test]
  (flush) ; Make sure nothing buffered
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))))

(deftest ^:transfer ^:network transfer-bridge
  (run-test! bridge-test))

(deftest ^:transfer ^:network transfer-isolate-node
  (run-test! isolate-node-test))

(deftest ^:transfer ^:network transfer-halves
  (run-test! halves-test))

(deftest ^:transfer ^:crash transfer-crash-subset
  (run-test! crash-subset-test))

(deftest ^:transfer ^:steady transfer-flush-compact
  (run-test! flush-compact-test))

;(deftest ^:transfer ^:clock transfer-clock-drift
;  (run-test! clock-drift-test))

(deftest ^:transfer ^:network ^:bootstrap transfer-bridge-bootstrap
  (run-test! bridge-test-bootstrap))

(deftest ^:transfer ^:network ^:bootstrap transfer-isolate-node-bootstrap
  (run-test! isolate-node-test-bootstrap))

(deftest ^:transfer ^:network ^:bootstrap transfer-halves-bootstrap
  (run-test! halves-test-bootstrap))

(deftest ^:transfer ^:crash ^:bootstrap transfer-crash-subset-bootstrap
  (run-test! crash-subset-test-bootstrap))

;(deftest ^:transfer ^:clock transfer-clock-drift-bootstrap
;  (run-test! clock-drift-test-bootstrap))

(deftest ^:transfer ^:network ^:decommission transfer-bridge-decommission
  (run-test! bridge-test-decommission))

(deftest ^:transfer ^:network ^:decommission transfer-isolate-node-decommission
  (run-test! isolate-node-test-decommission))

(deftest ^:transfer ^:network ^:decommission transfer-halves-decommission
  (run-test! halves-test-decommission))

(deftest ^:transfer ^:crash ^:decommission transfer-crash-subset-decommission
  (run-test! crash-subset-test-decommission))

;(deftest ^:transfer ^:clock ^:decommission transfer-clock-drift-decommission
;  (run-test! clock-drift-test-decommission))

(deftest ^:transfer ^:network ^:mix transfer-bridge-mix
  (run-test! bridge-test-mix))

(deftest ^:transfer ^:network ^:mix transfer-isolate-node-mix
  (run-test! isolate-node-test-mix))

(deftest ^:transfer ^:network ^:mix transfer-halves-mix
  (run-test! halves-test-mix))

(deftest ^:transfer ^:crash ^:mix transfer-crash-subset-mix
  (run-test! crash-subset-test-mix))

;(deftest ^:transfer ^:clock ^:mix transfer-clock-drift-mix
;  (run-test! clock-drift-test-mix))
