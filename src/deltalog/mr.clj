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

; This creates a tap that will read a text file line-by-line.
; The resultant field will be named "line".
(def in-tap (Hfs. (TextLine. (make-fields "line")) "example/input.json"))

; This creates a JSON parser that will extract the "name" and "email"
; from a JSON object. The resultant fields will be named "name" and "email".
(def splitter (JSONSplitter. (make-fields "name" "email") (into-array ["name" "email"])))

(def out-tap (Hfs. (TextLine.) "example/output" true))

(defn -main []
  (let [parent-pipe (Pipe. "json_split")
        splitter-pipe (Each. parent-pipe splitter)]

    (.complete (.connect (HadoopFlowConnector.) in-tap out-tap splitter-pipe))))
