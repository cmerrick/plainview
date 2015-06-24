(ns plainview.core
  (:gen-class)
  (:require [plainview.producer :as producer]
            [plainview.sql :as sql]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.pprint :as pprint]))

(def table-map (atom {}))
(def column-map (atom {}))

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
   ["-i" "--server-id ID" "Unique ID for this MySQL repication client"
    :parse-fn #(Integer/parseInt %)]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defmulti on-event :type)
(defmethod on-event :query
  [e]
  (when (re-matches #"(?i).*ALTER\s+TABLE.*" (:sql e))
    (println "INVALIDATE CACHE!")))
(defmethod on-event :table-map
  [e]
  (swap! table-map assoc (:table-id e) (select-keys e [:table :database])))
(defmethod on-event :write-rows
  [e]
  (let [table (:table (get @table-map (:table-id e)))
        db (:database (get @table-map (:table-id e)))]
    (pprint/pprint {:table table
                    :columns (map :column_name (get @column-map {:table_name table :table_schema db}))
                    :rows (:rows e)})))
(defmethod on-event :default
  [e]
  ())

(def callback (fn [e] (on-event e)))

(defn -main [& args]
  (let [args' (if (= "plainview.core" (first args))
                (next args)
                args)
        {:keys [options arguments errors summary]} (parse-opts args' cli-options)]
    (cond
     errors (exit 1 (error-msg errors))
     (nil? (:username options)) (exit 1 "A replication username must be specified")
     (nil? (:password options)) (exit 1 "A replication password must be specified")
     (nil? (:server-id options)) (exit 1 "A server-id name must be specified"))
    (reset! column-map (sql/columns (sql/spec options)))
    (producer/connect! (producer/replication-client options callback))))
