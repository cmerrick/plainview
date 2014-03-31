(ns plainview.consumer
  (:require [cheshire.core :refer :all]
            [amazonica.aws.kinesis :as kinesis]
            [amazonica.aws.s3 :as s3]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class))

(defn- to-json [byte-buffer]
  (let [b (byte-array (.remaining byte-buffer))]
    (.get byte-buffer b)
    (parse-string (String. b "UTF-8"))))

(defn- to-bytes [string]
  (.getBytes string "UTF-8"))

(defn- to-stream [bytes]
  (java.io.ByteArrayInputStream. bytes))

(defn vec->json [rows]
  (->> rows
       (map generate-string)
       (clojure.string/join \newline)))

(defn flatten-records [records]
  (for [record records
        row (get-in record [:data "data"])]
    (assoc (:data record) "data" row "sequence-number" (:sequence-number record))))

(defn s3-emitter [bucket records]
  (let [flattened (flatten-records records)
        by-table (group-by #(str (get % "database") "." (get % "table")) flattened)]
    (doseq [[fqtn table-rows] (map identity by-table)]
      (let [sample (first table-rows)
            filename (str (get sample "sequence-number"))
            database (get sample "database")
            table (get sample "table")
            bytes (to-bytes (vec->json (map #(dissoc % "sequence-number") table-rows)))]
        (s3/put-object :bucket-name (str bucket "/" database "/" table)
                       :key filename
                       :input-stream (to-stream bytes)
                       :metadata {:content-length (count bytes)}))))
  nil)

(defn- create-worker [{:keys [app bucket stream] :as options}]
  (kinesis/worker! :app app
                   :stream stream
                   :checkpoint false ;; default to disabled checkpointing, can still force
                   ;; a checkpoint by returning true from the processor function
                   :deserializer to-json
                   :processor (partial s3-emitter bucket)))

(def cli-options
  [["-a" "--app APPLICATION" "Kinesis application name"]
   ["-b" "--bucket BUCKET" "S3 bucket to write to"]
   ["-s" "--stream STREAM" "Kinesis stream name"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (clojure.string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
     errors (exit 1 (error-msg errors))
     (nil? (:app options)) (exit 1 "An application name must be specified")
     (nil? (:bucket options)) (exit 1 "A bucket name must be specified")
     (nil? (:stream options)) (exit 1 "A kinesis stream name must be specified"))
    (s3/create-bucket (:bucket options))
    (create-worker options)))
