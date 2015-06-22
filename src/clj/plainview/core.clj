(ns plainview.core
  (:gen-class)
  (:require [plainview.producer :as producer]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as string]
            [clojure.pprint :as pprint]))

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

(def callback (fn [event] (pprint/pprint event)))

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
    (producer/connect! (producer/replication-client options callback))))
