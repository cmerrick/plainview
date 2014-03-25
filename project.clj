(defproject repwrite "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["conjars" {:url "http://conjars.org/repo"}]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [open-replicator/open-replicator "1.0.5"]
                 [org.apache.hadoop/hadoop-core "1.1.2"]
                 [cascading/cascading-hadoop "2.2.0"]
                 [cascading/cascading-local "2.2.0"]
                 [cascading.gmarabout/cascading-json "0.0.3"]
                 [cheshire "5.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [cascading.avro/avro-scheme "2.1.1"
                  :exclusions [[org.slf4j/slf4j-log4j12]
                               [org.apache.hadoop/hadoop-core]
                               cascading/cascading-core]]
                 [org.apache.avro/avro "1.7.4" :exclusions [org.slf4j/slf4j-api]]
                 [com.damballa/abracad "0.4.9"]
                 [midje "1.6.3"]
                 [amazonica "0.2.10"]])
