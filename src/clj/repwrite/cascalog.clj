(ns repwrite.cascalog
  (:require [cascalog.api :refer :all]
            [cheshire.core :refer :all]
            [cascalog.logic.def :as def]))

(def/defmapfn parse-json [line]
  "reads in a line of string and splits it by a regular expression"
  (let [row (parse-string line)]
    (concat [(get row "timestamp") (get row "tombstone") (get row "tableid")] (get row "data"))))

(def input-file (hfs-textline "example/input.json"))

(defn do-cascalog []
 (?<- (stdout) [?timestamp ?del ?table ?id ?name]
      (input-file :> ?line)
      (parse-json ?line :> ?timestamp ?del ?table ?id ?name _ _)))

(defn -main
  [& args]
  (do-cascalog))
