(defproject plainview "0.1.0-SNAPSHOT"
  :description "A toolkit for building a data pipeline from your database's binary log stream."
  :url "http://cmerrick.github.io"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :repositories [["conjars" {:url "http://conjars.org/repo"}]]
  :source-paths ["src/clj"]
  :test-paths ["test/clj/"]
  :main plainview.core
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [open-replicator/open-replicator "1.0.5"]
                 [cheshire "5.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [midje "1.6.3"]
                 [com.github.shyiko/mysql-binlog-connector-java "0.2.1"]]
  :plugins [[lein-midje "3.0.1"]])
