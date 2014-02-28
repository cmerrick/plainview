(ns deltalog.mr
  (:require [deltalog.schema :as schema])
  (:import [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [cascading.tap.hadoop Hfs]
           [cascading.avro AvroScheme]
           [cascading.scheme.hadoop TextDelimited TextLine]
           [cascading.tuple Fields]
           [cascading.operation.expression ExpressionFilter]
           [cascading.pipe Pipe Each GroupBy Every]
           [cascading.operation.aggregator Last] ; for learning
           [cascading.flow Flow FlowDef]
           [cascading.flow.hadoop HadoopFlowConnector]
           [cascading.json.operation JSONSplitter JSONFlatten]))

(defn make-fields [fields] (Fields. (into-array fields)))

(defn make-splitter
  "Creates a JSON parser that will extract each field from the input
  JSON object. The resultant fields will have the same names as they do
  in the json object."
  [json-paths]
  (JSONSplitter. (make-fields json-paths) (into-array json-paths)))

; This creates a tap that will read a text file line-by-line.
; The resultant field will be named "line".
(def in-tap (Hfs. (TextLine. (make-fields ["line"])) "example/input.json"))

(def out-tap (Hfs. (AvroScheme. (schema/avro-schema)) "example/output" true))

(defn filter-ts-if
  "If timestamp evaluates to true, then this will create a new
  pipe that filters where the 'timestamp' field is greater than or equal
  to the given timestamp value. Otherwise it returns pipe."
  [pipe timestamp]
  (if timestamp
    (Each. pipe
           (make-fields ["timestamp"])
           (ExpressionFilter. (str "timestamp >= " timestamp) Long/TYPE))
    pipe))

(defn -main
  [& args]
  (let [json-paths ["id" "timestamp" "is_delete" "data"]

        splitter-pipe (Each. "json_split" (make-splitter json-paths))
        tail-pipe (-> splitter-pipe
                      (filter-ts-if (first args))
                      (GroupBy. (make-fields ["id"])
                                (make-fields ["timestamp"]))
                      (Every. Fields/ALL (Last.) Fields/RESULTS))

        flow-def (-> (FlowDef/flowDef)
                     (.addSource splitter-pipe in-tap)
                     (.addTailSink tail-pipe out-tap))]

    (-> (HadoopFlowConnector.)
        (.connect flow-def)
        (.complete))))
