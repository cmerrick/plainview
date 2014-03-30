(ns plainview.core-test
  (:use midje.sweet)
  (:require [plainview.producer :refer :all])
  (:import [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent BinlogEventV4HeaderImpl]
           [com.google.code.or.common.glossary Row Pair]
           [com.google.code.or.common.glossary.column Int24Column]))

(def header (doto (BinlogEventV4HeaderImpl.)
              (.setTimestamp (long 10000))
              (.setTimestampOfReceipt (long 10000))
              (.setServerId (long 1))
))

(defn int-col [i] (Int24Column/valueOf i))

(defn to-row [cols] (Row. cols))

(defn to-pair [before after] (Pair. before after))

(facts "row-based replication events"
       (fact "it handles empty WriteRowsEvents"
             (parse-event-data  (WriteRowsEvent. header)) => [])

       (fact "it handles single-row WriteRowsEvents"
             (let [e (doto (WriteRowsEvent. header)
                       (.setRows [(to-row [(int-col 1)])]))]
               (parse-event-data e) => [[1]]))

       (fact "it handles multi-row WriteRowsEvents"
             (let [e (doto (WriteRowsEvent. header)
                       (.setRows [(to-row [(int-col 1)])
                                  (to-row [(int-col 2)])]))]
               (parse-event-data e) => [[1]
                                   [2]]))

       (fact "it handles single-row UpdateRowsEvents"
             (let [e (doto (UpdateRowsEvent. header)
                       (.setRows [(to-pair (to-row [(int-col 1)])
                                           (to-row [(int-col 2)]))]))]
               (parse-event-data e) => [[2]]))

       (fact "it handles multi-row UpdateRowsEvents"
             (let [e (doto (UpdateRowsEvent. header)
                       (.setRows [(to-pair (to-row [(int-col 1)])
                                           (to-row [(int-col 2)]))
                                  (to-pair (to-row [(int-col 3)])
                                           (to-row [(int-col 4)]))]))]
               (parse-event-data e) => [[2]
                                   [4]]))

       (fact "it handles single-row DeleteRowsEvents"
             (let [e (doto (DeleteRowsEvent. header)
                       (.setRows [(to-row [(int-col 1)])]))]
               (parse-event-data e) => [[1]]))

       (fact "it handles multi-row DeleteRowsEvents"
             (let [e (doto (DeleteRowsEvent. header)
                       (.setRows [(to-row [(int-col 1)])
                                  (to-row [(int-col 2)])]))]
               (parse-event-data e) => [[1]
                                   [2]])))
