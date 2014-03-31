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
  ;; Not important, but you can use when-not or not-empty.
  (when (not (empty? data))
    (kinesis/put-record
     stream
     (string->buff (generate-string event)) tableid))) ;; probably shouldn't use tableid as partition key


;; Is this a single-threaded program? If so, an atom is
;; appropriate. If not, you should use a ref.
(def table-map (atom {}))

;; Does "_unknown" have some kind of special meaning? I would document
;; this.
(defn query-table-map [tableid]
  (get @table-map tableid {:database "_unknown" :table "_unknown"}))

;; I would probably not use a multimethod for this. Since there's only
;; one implementation, I would just use a regular fn. It would
;; simplify this a bit.
(defmulti pre-parse-event class)
(defmethod pre-parse-event TableMapEvent
  [e]
  ;; You don't need to make an anonymous function here. `(swap! table-map assoc ... )` will do `(assoc @table-map  ...)`
  (swap! table-map #(assoc %
                      (.getTableId e)
                      {:database (coerce (.getDatabaseName e)) :table (coerce (.getTableName e))})))

(defmethod pre-parse-event :default
  [e]
  nil)

;; I'm not sure that I'd use multimethods for this either, since
;; there's a small number of events, their implementations are very
;; simple, and they're all defined in one place.
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

;; Also probably wouldn't use a multimethod for this. Especially since
;; the implementations are so similar.
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

;; Does the "--host HOST" argument syntax work? 
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

;; You can maybe rename this to "fail" or something and get rid of
;; status, since you only ever call it with status 1. 
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
