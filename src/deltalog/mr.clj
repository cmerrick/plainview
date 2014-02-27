(ns deltalog.mr
  (:import [org.apache.hadoop.fs Path FileSystem]
           [org.apache.hadoop.conf Configuration]
           [cascading.tap.hadoop Hfs]
           [cascading.scheme.hadoop TextDelimited TextLine]
           [cascading.tuple Fields]
           [cascading.pipe Pipe Each]
           [cascading.flow Flow FlowDef]
           [cascading.flow.hadoop HadoopFlowConnector]
           [cascading.json.operation JSONSplitter JSONFlatten]))

(defn make-fields [& fields] (Fields. (into-array fields)))

(def source-scheme (TextLine. (make-fields "line")))
(def in-tap (Hfs. source-scheme "example/input.json"))
(def out-tap (Hfs. (TextLine.) "example/output" true))

(def splitter (JSONSplitter. (make-fields "name" "email") (into-array ["name" "email"])))

(defn -main []
  (let [pipe1 (Pipe. "json_flatten")
        pipe2 (Each. pipe1 (make-fields "line") splitter (make-fields "name" "email"))
        flow-connector (HadoopFlowConnector.)]

  (.complete (.connect flow-connector in-tap out-tap pipe2))))
