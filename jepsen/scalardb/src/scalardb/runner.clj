(ns scalardb.runner
  (:gen-class)
  (:require [cassandra
             [core :as cassandra]
             [runner :as car]
             [nemesis :as can]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardb.transfer]
            [scalardb.transfer_append]))

(def tests
  "A map of test names to test constructors."
  {"transfer"        scalardb.transfer/transfer-test
   "transfer-append" scalardb.transfer_append/transfer-append-test})

(def opt-spec
  [(cli/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   (cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [`(can/none)]
                     car/nemeses)

   (cli/repeated-opt nil "--join NAME" "Which node joinings to use"
                     [{:name "" :bootstrap false :decommission false}]
                     car/joinings)

   (cli/repeated-opt nil "--clock NAME" "Which clock-drift to use"
                     [{:name "" :bump false :strobe false}]
                     car/clocks)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (cli/tarball-opt "https://archive.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz")])

(defn test-cmd
   []
   {"test" {:opt-spec (into cli/test-opt-spec opt-spec)
            :opt-fn   (fn [parsed] (-> parsed cli/test-opt-fn))
            :usage    (cli/test-usage)
            :run      (fn [{:keys [options]}]
                        (doseq [i (range (:test-count options))
                                test-fn (:test options)
                                nemesis (:nemesis options)
                                joining (:join options)
                                clock (:clock options)]
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
  (cli/run! (test-cmd)
            args))
