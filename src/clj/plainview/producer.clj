(ns plainview.producer
  (:require [plainview.coerce :refer [coerce]]
            [cheshire.core :refer :all]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [amazonica.aws.kinesis :as kinesis])
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent AbstractRowEvent])
  (:gen-class))

(defn- string->buff [s]
  (-> (.getBytes s "utf-8")
      (java.nio.ByteBuffer/wrap)))

(defn write-kinesis [stream {:keys [data tableid] :as event}]
  (when (not (empty? data))
    (kinesis/put-record
     stream
     (string->buff (generate-string event)) tableid))) ;; probably shouldn't use tableid as partition key

(def table-map (atom {}))

(defn query-table-map [tableid]
  (get @table-map tableid {:database "_unknown" :table "_unknown"}))

(defmulti pre-parse-event class)
(defmethod pre-parse-event TableMapEvent
  [e]
  (swap! table-map #(assoc %
                      (.getTableId e)
                      {:database (coerce (.getDatabaseName e)) :table (coerce (.getTableName e))})))

(defmethod pre-parse-event :default
  [e]
  nil)

(defmulti parse-event-data class)
(defmethod parse-event-data WriteRowsEvent
  [e]
  (map #(map coerce (.getColumns %)) (.getRows e)))
(defmethod parse-event-data UpdateRowsEvent
  [e]
  (map #(map coerce (.getColumns (.getAfter %))) (.getRows e)))
(defmethod parse-event-data DeleteRowsEvent
  [e]
  (map #(map coerce (.getColumns %)) (.getRows e)))
(defmethod parse-event-data :default
  [e]
  (println (str e "\n"))
  {})

(defmulti parse-meta-data class)
(defmethod parse-meta-data AbstractRowEvent
  [e]
  (let [tableid (.getTableId e)
        header {:tableid tableid
                :timestamp (.getTimestamp (.getHeader e))
                :tombstone false}]
    (merge header (query-table-map tableid))))
(defmethod parse-meta-data DeleteRowsEvent
  [e]
  (let [tableid (.getTableId e)
        header {:tableid tableid
                :timestamp (.getTimestamp (.getHeader e))
                :tombstone true}]
    (merge header (query-table-map tableid))))
(defmethod parse-meta-data :default
  [e]
  {})

(deftype MyListener [stream]
  BinlogEventListener
  (onEvents
    [this e]
    (pre-parse-event e)
    (write-kinesis stream (conj {:data (parse-event-data e)} (parse-meta-data e)))))

(defn replicator
  "get a replicator"
  [{:keys [username password host port filename position] :as opts} listener]
  (doto (OpenReplicator.)
    (.setUser username)
    (.setPassword password)
    (.setHost host)
    (.setPort port)
    (.setServerId 1234)
    (.setBinlogFileName filename)
    (.setBinlogPosition position)
    (.setBinlogEventListener listener)))

(def cli-options
  [["-h" "--host HOST" "Replication master hostname or IP"
    :default "127.0.0.1"]
   ["-P" "--port PORT" "Replication master port number"
    :default 3306
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-u" "--username USERNAME" "MySQL username"]
   ["-p" "--password PASSWORD" "MySQL password"]
   ["-f" "--filename FILENAME" "Binlog filename"]
   ["-n" "--position POSITION" "Binlog position"
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--stream STREAM" "Kinesis stream name"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
     errors (exit 1 (error-msg errors))
     (nil? (:filename options)) (exit 1 "A replication filename must be specified")
     (nil? (:position options)) (exit 1 "A replication position must be specified")
     (nil? (:username options)) (exit 1 "A replication username must be specified")
     (nil? (:password options)) (exit 1 "A replication password must be specified")
     (nil? (:stream options)) (exit 1 "A kinesis stream name must be specified"))
    (.start (replicator options (MyListener. (:stream options))))))
