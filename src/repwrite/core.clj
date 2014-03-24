(ns repwrite.core
  (:require [repwrite.coerce :refer [coerce]]
            [repwrite.schema :as schema]
            [cheshire.core :refer :all]
            [abracad.avro :as avro]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]])
  (:import [com.google.code.or OpenReplicator]
           [com.google.code.or.binlog BinlogEventListener]
           [com.google.code.or.binlog.impl.event WriteRowsEvent UpdateRowsEvent
            TableMapEvent QueryEvent DeleteRowsEvent AbstractRowEvent])
  (:gen-class))

#_(defn root-dir [table-id] (str "example/raw/" table-id))

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

(defn write-json [{:keys [data] :as event}]
  (when (not (empty? data))
    (let [filename (str root-dir "raw.json")]
      (spit filename (str (generate-string data) "\n") :append true))))

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
  {:table-id (.getTableId e)})
(defmethod parse-meta-data :default
  [e]
  {})

(deftype MyListener []
  BinlogEventListener
  (onEvents
    [this e]
    (write-json (conj (parse-meta-data e)
                     {:data (parse-event-data e)}))))

(defn replicator
  "get a replicator"
  [{:keys [host port filename position] :as opts} listener]
  (doto (OpenReplicator.)
    (.setUser "replication-user")
    (.setPassword "password")
    (.setHost host)
    (.setPort port)
    (.setServerId 1234)
    (.setBinlogFileName filename)
    (.setBinlogPosition position)
    (.setBinlogEventListener listener)))

(def cli-options
  [["-h" "--host HOST" "Replication master hostname or IP"
    :default "127.0.0.1"]
   ["-p" "--port PORT" "Replication master port number"
    :default 3306
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
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
     (nil? (:position options)) (exit 1 "A replication position must be specified"))
    (mk-dir root-dir)
    (.start (replicator options (MyListener.)))))
