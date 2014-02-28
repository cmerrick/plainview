(ns deltalog.core
  (:require [cheshire.core :refer :all]
            [deltalog.coerce :refer [coerce]])
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent])
  (:gen-class))

(def root-dir "/Users/chris/raw/")

(defn write-data [table-id ts pk delete? data]
  (let [ out {:timestamp ts
              :id (coerce pk)
              :is_delete delete?
              :data (map coerce data)}]
    (spit (str root-dir table-id) (str (generate-string out) "\n") :append true)))

(defmulti handle-event class)

(defmethod handle-event WriteRowsEvent
  [e]
  (doseq [row (.getRows e)]
    (let [columns (.. row getColumns)]
      (write-data (.getTableId e)
                  (.. e getHeader getTimestamp)
                  (.get columns 0)          ; assume pk is first
                  0
                  columns))))

(defmethod handle-event UpdateRowsEvent
  [e]
  (doseq [pair (.getRows e)]
    (let [columns (.. pair getAfter getColumns)]
      (write-data (.getTableId e)
                  (.. e getHeader getTimestamp)
                  (.get columns 0)          ; assume pk is first
                  0
                  columns))))

(defmethod handle-event DeleteRowsEvent
  [e]
  (doseq [row (.getRows e)]
    (let [columns (.. row getColumns)]
      (write-data (.getTableId e)
                  (.. e getHeader getTimestamp)
                  (.get columns 0)          ; assume pk is first
                  1
                  columns))))

(defmethod handle-event :default
  [e]
;  (println (str e "\n"))
  )

(deftype MyListener []
  BinlogEventListener
  (onEvents
    [this e]
    (handle-event e)))

(defn replicator
  "get a replicator"
  [binlog-file binlog-pos listener]
  (doto (OpenReplicator.)
    (.setUser "replication-user")
    (.setPassword "password")
    (.setHost "127.0.0.1")
    (.setPort 5001)
    (.setServerId 1234)
    (.setBinlogFileName binlog-file)
    (.setBinlogPosition binlog-pos)
    (.setBinlogEventListener listener)))

(defn -main []
  (.start (replicator "mysql-bin.000001" 4 (MyListener.))))
