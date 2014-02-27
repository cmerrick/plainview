(defproject deltalog"0.1.0-SNAPSHOT"
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
                 [cheshire "5.3.1"]])
