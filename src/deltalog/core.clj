(ns deltalog.core
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent])
  (:gen-class))

(def root-dir "/Users/chris/raw/")

(defn write-rows-event [event]
  (let [table-id (.getTableId event)
        out (str "t=" (.. event getHeader getTimestamp) " " event "\n")]
    (spit (str root-dir table-id) out :append true)))

(defn write-schema-event []
  (let [out (str "t=" (.. event getHeader getTimestamp) " " event "\n")]
    (spit (str root-dir "schema") out :append true)))

(defmulti handle-event class)

(defmethod handle-event WriteRowsEvent
  [e]
  (write-rows-event e))

(defmethod handle-event UpdateRowsEvent
  [e]
  (write-rows-event e))

(defmethod handle-event DeleteRowsEvent
  [e]
  (write-rows-event e))

(defmethod handle-event TableMapEvent
  [e]
  (write-schema-event (.getTableId e) (.. e getHeader getTimestamp) e))

(defmethod handle-event QueryEvent
  [e]
  (write-schema-event (.getTableId e) (.. e getHeader getTimestamp) e))

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
  (.start (replicator "mysql-bin.000001" 4 (MyListener.))))
