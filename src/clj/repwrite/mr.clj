(ns repwrite.mr
  (:require [repwrite.schema :as schema])
  (:import [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [cascading.tap SinkMode]
           [cascading.tap.hadoop Hfs Lfs]
           [cascading.avro AvroScheme]
           [cascading.scheme.hadoop TextDelimited TextLine]
           [cascading.tuple Fields]
           [cascading.operation.expression ExpressionFilter]
           [cascading.pipe Pipe Each GroupBy Every]
           [cascading.operation.aggregator Last] ; for learning
           [cascading.flow Flow FlowDef]
           [cascading.flow.hadoop HadoopFlowConnector]
           [cascading.json.operation JSONSplitter JSONFlatten]))

(defn fields [fields] (Fields. (into-array fields)))

; This creates a tap that will read a text file line-by-line.
; The resultant field will be named "line".
(def avro-in-tap (Hfs. (AvroScheme.) "example/raw"))
(def avro-out-tap (Hfs. (AvroScheme. schema/user-schema) "example/output" true))

; This creates a tap that will read a text file line-by-line.
; The resultant field will be named "line".
(def json-in-tap (Hfs. (TextLine. (fields ["line"])) "example/input.json"))
(def json-out-tap (Hfs. (TextLine.) "example/output" true))

(defn make-splitter
  "Creates a JSON parser that will extract each field from the input
  JSON object. The resultant fields will have the same names as they do
  in the json object."
  [json-paths]
  (JSONSplitter. (fields json-paths) (into-array json-paths)))

(defn filter-ts-if
  "If timestamp evaluates to true, then this will create a new
  pipe that filters where the 'timestamp' field is greater than or equal
  to the given timestamp value. Otherwise it returns pipe."
  [pipe timestamp]
  (if timestamp
    (Each. pipe
           (fields ["timestamp"])
           (ExpressionFilter. (str "timestamp >= " timestamp) Long/TYPE))
    pipe))

(defn -main
  [& args]
  (let [json-paths ["timestamp" "tombstone" "tableid" "data"]
        splitter-pipe (Each. "json_split" (make-splitter json-paths))

        tail-pipe (-> splitter-pipe
                      #_(filter-ts-if (first args))
                      #_(GroupBy. (fields ["timestamp"]))
                      #_(Every. Fields/ALL (Last.) Fields/RESULTS)
                      )

        flow-def (-> (FlowDef/flowDef)
                     (.addSource splitter-pipe json-in-tap)
                     (.addTailSink tail-pipe json-out-tap))]

    (let [flow (.connect (HadoopFlowConnector.) flow-def)]
      (doto flow
        (.writeDOT "example/flow.dot")
        (.complete)))))
