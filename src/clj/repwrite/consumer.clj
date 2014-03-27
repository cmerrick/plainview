(ns repwrite.consumer
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

(defn stdout-emitter [records]
  (doseq [row records]
    (println (:data row)
             (:sequence-number row)
             (:partition-key row)))
  records)

(defn records->string [records]
  (let [all-rows (for [record records
                       row (get-in record [:data "data"])]
                   (assoc (:data record) "data" row))]
    (->> all-rows
        (map generate-string)
        (clojure.string/join \newline))))

(defn s3-emitter [bucket records]
  (let [tableid (:partition-key (first records))
        filename (str (:sequence-number (first records)) "-" (:sequence-number (last records)))
        bytes (to-bytes (records->string records))]
    (s3/put-object :bucket-name (str bucket "/" tableid)
                   :key filename
                   :input-stream (to-stream bytes)
                   :metadata {:content-length (count bytes)})))

(defn- create-worker [{:keys [app bucket stream] :as options}]
  (kinesis/worker! :app app
                   :stream stream
                   :checkpoint false ;; default to disabled checkpointing, can still force
                   ;; a checkpoint by returning true from the processor function
                   :deserializer to-json
                   :processor (comp (partial s3-emitter bucket) stdout-emitter)))

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
    (create-worker options)))
