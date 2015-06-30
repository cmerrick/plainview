(ns plainview.core
  (:gen-class)
  (:require [plainview.producer :as producer]
            [plainview.sql :as sql]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [cheshire.core :refer :all]
            [clojure.set :refer [rename-keys]]
            [rjmetrics.core :as rjm]
            [clj-http.client :as http]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.java.jmx :as jmx]))

(def table-map (atom {}))
(def table-filter-fn (atom (constantly false)))

(defn event-push-log
  [{:keys [table data]}]
  (when-not (@table-filter-fn table)
    (log/info (str "[SecondsBehindMaster" (:SecondsBehindMaster (jmx/mbean "mysql.binlog:type=BinaryLogClientStatistics")) "]") table (generate-string data) )))

(defn event-push-rjm
  [{:keys [table data]}]
  (rjm/push-data {:client-id 1 :api-key ""}
                 table
                 data
                 rjm/SANDBOX-BASE))

;todo
(defn- apply-col-mask
  [col-mask table]
  table)

(defn encode-fn
  [type]
  (case type
    (:decimal :tiny :short :long :float :double :null :longlong :int :bit :newdecimal) identity
    (:timestamp :date :time :datetime :year :newdate :timestamp-v2 :datetime-v2 :time-v2) str
    (:blob) #(String. % java.nio.charset.StandardCharsets/UTF_8)
    str))

(defn encode-row
  [types row]
  (map #(if (nil? %2)
          %2
          ((encode-fn %1) %2)) types row))

(defn format-for-rjm
  [{:keys [timestamp table-id cols rows type deleted?] :as e}]
  (let [{:keys [table database column-types]} (get @table-map table-id)
        fields (apply-col-mask cols
                (sql/get-columns! {:table_name table :table_schema database}))
        key-fields (vec (map :column_name
                             (filter #(= (:column_key %) "PRI") fields)))]
    {:table (str database "." table)
     :data (map #(as->
                  (encode-row column-types %) $
                  (zipmap (map :column_name fields) $)
                  (assoc $ "timestamp" timestamp
                           "deleted?" deleted?
                           "keys" key-fields))
                rows)}))

(defmulti on-event :type)
(defmethod on-event :query
  [e]
  (when (re-matches #"(?i)\s*(ALTER|CREATE)\s+TABLE.*" (:sql e))
    (sql/reset-columns!)))
(defmethod on-event :table-map
  [e]
  (swap! table-map assoc (:table-id e)
         (select-keys e [:table :database :column-types])))
(defmethod on-event :write-rows
  [e]
  (event-push-log (format-for-rjm (assoc e :deleted? false))))
(defmethod on-event :delete-rows
  [e]
  (event-push-log (format-for-rjm (assoc e :deleted? true))))
(defmethod on-event :update-rows
  [e]
  (event-push-log
   (format-for-rjm
    (-> (assoc e :deleted? false)
        (rename-keys {:cols-new :cols})
        (update-in [:rows] (partial map second))))))
(defmethod on-event :default
  [e]
  ())

(def callback
  (fn [e]
    (on-event e)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

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
    :parse-fn #(Integer/parseInt %)]
   ["-r" "--filter-regex REGEX" "Regex to filter out tables, where tables are represented as 'dbname.tablename'"
    :default #"a^" ; matches nothing
    :parse-fn re-pattern]])

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
    (sql/configure! options)
    (reset! table-filter-fn #(re-matches (:filter-regex options) %))
    (producer/connect!
     (producer/replication-client options callback))))
