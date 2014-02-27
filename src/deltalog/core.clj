(ns deltalog.core
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent])
  (:gen-class))


(defn write-event [table timestamp value]
  (let [out (str "t=" timestamp " " value "\n")]
;    (spit (str "/Users/chris/raw/" table) out :append true)
    (print out)))

(defmulti handle-event class)

(defmethod handle-event WriteRowsEvent
  [e]
  (write-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event UpdateRowsEvent
  [e]
  (write-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event DeleteRowsEvent
  [e]
  (write-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event TableMapEvent
  [e]
  (write-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event QueryEvent
  [e]
  (write-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event :default
  [e]
  (println (str e "\n")))

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
  (.start (replicator "mysql-bin.000001" 5601 (MyListener.))))
