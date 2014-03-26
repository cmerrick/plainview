(ns repwrite.core
  (:require [repwrite.coerce :refer [coerce]]
            [repwrite.schema :as schema]
            [cheshire.core :refer :all]
            [abracad.avro :as avro]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [amazonica.aws.kinesis :as kinesis])
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent AbstractRowEvent])
  (:gen-class))

(def root-dir (str "example/raw/"))

(defn mk-dir [dir]
  (.mkdir (java.io.File. dir))
  dir)

(defn write-avro [{:keys [table-id]} data]
  (let [filename (str (mk-dir (root-dir table-id)) "/raw.avro")
        schema (schema/tableid->schema table-id)]
    (with-open [adf (if (.exists (clojure.java.io/as-file filename))
                      (avro/data-file-writer filename)
                      (avro/data-file-writer schema filename))]
      (.append adf data))))

(defn- string->buff [s]
  (-> (.getBytes s "utf-8")
      (java.nio.ByteBuffer/wrap)))

(defn write-json [{:keys [data tableid] :as event}]
  (when (not (empty? data))
    (let [filename (str root-dir tableid ".json")]
      (spit filename (str (generate-string event) "\n") :append true))))

(defn write-kinesis [{:keys [data tableid] :as event}]
  (when (not (empty? data))
    (kinesis/put-record "chris-rep" (string->buff (generate-string event)) data tableid)))

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
  {:tableid (.getTableId e)
   :timestamp (.getTimestamp (.getHeader e))
   :tombstone false})
(defmethod parse-meta-data DeleteRowsEvent
  [e]
  {:tableid (.getTableId e)
   :timestamp (.getTimestamp (.getHeader e))
   :tombstone true})
(defmethod parse-meta-data :default
  [e]
  {})

(deftype MyListener []
  BinlogEventListener
  (onEvents
    [this e]
    (write-kinesis (conj {:data (parse-event-data e)} (parse-meta-data e)))))

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
    :parse-fn #(Integer/parseInt %)]])

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
     (nil? (:password options)) (exit 1 "A replication password must be specified"))
    (mk-dir root-dir)
    (.start (replicator options (MyListener.)))))
