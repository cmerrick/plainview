(ns deltalog.core
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent])
  (:gen-class))


(defn write-rows [table timestamp rows]
  (spit (str "/Users/chris/raw/" table) (str "t=" timestamp " "(clojure.string/join ", " (map #(.getColumns %) rows)) "\n") :append true))

(defmulti handle-event class)

(defmethod handle-event WriteRowsEvent
  [e]
  (write-rows (.getTableId e) (.. e getHeader getTimestamp) (.getRows e)))

(defmethod handle-event UpdateRowsEvent
  [e]
  (write-rows (.getTableId e) (.. e getHeader getTimestamp) (.. e getRows getAfter)))

(defmethod handle-event :default
  [e]
  (println "skip"))

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
  (.start (replicator "mysql-bin.000002" 2129629 (MyListener.))))
