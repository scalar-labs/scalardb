(ns scalardb.runner
  (:gen-class)
  (:require [jepsen
             [core :as jepsen]
             [cli :as jc]]
            [cassandra
             [core    :as cassandra]
             [runner  :as car]
             [nemesis :as can]]
            [scalardb.transfer]
            [scalardb.transfer_append]))

(def tests
  "A map of test names to test constructors."
  {"transfer"        scalardb.transfer/transfer-test
   "transfer_append" scalardb.transfer_append/transfer-append-test})

(def opt-spec
  [(jc/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                    [`(can/none)]
                    car/nemeses)

   (jc/repeated-opt nil "--join NAME" "Which node joinings to use"
                    [{:name "" :bootstrap false :decommission false}]
                    car/joinings)

   (jc/repeated-opt nil "--clock NAME" "Which clock-drift to use"
                    [{:name "" :bump false :strobe false}]
                    car/clocks)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (jc/tarball-opt "https://archive.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz")])

(defn test-cmd
   []
   {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
            :opt-fn (fn [parsed] (-> parsed jc/test-opt-fn))
            :usage (jc/test-usage)
            :run (fn [{:keys [options]}]
                   (doseq [i        (range (:test-count options))
                           test-fn  (:test options)
                           nemesis  (:nemesis options)
                           joining  (:join options)
                           clock    (:clock options)]
                     (let [test (-> options
                                    (car/combine-nemesis nemesis joining clock)
                                    (assoc :db (cassandra/db (:cassandra options)))
                                    (dissoc :test)
                                    test-fn
                                    jepsen/run!)]
                       (when-not (:valid? (:results test))
                         (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
