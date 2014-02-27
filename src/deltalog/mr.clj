(ns deltalog.mr
  (:import [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [cascading.tap.hadoop Hfs]
           [cascading.scheme.hadoop TextDelimited TextLine]
           [cascading.tuple Fields]
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

(def out-tap (Hfs. (TextLine.) "example/output" true))

(defn -main []
  (let [json-paths ["id" "timestamp" "is-delete" "data"]

        splitter (make-splitter json-paths)
        splitter-pipe (Each. "json_split" splitter)
        group-pipe (GroupBy. splitter-pipe (make-fields ["id"]) (make-fields ["timestamp"]))
        max-pipe (Every. group-pipe Fields/ALL (Last.) Fields/RESULTS)

        flow-def (-> (FlowDef/flowDef)
                     (.addSource splitter-pipe in-tap)
                     (.addTailSink max-pipe out-tap))]

    (-> (HadoopFlowConnector.)
        (.connect flow-def)
        (.complete))))
