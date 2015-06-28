(defproject plainview "0.1.0-SNAPSHOT"
  :description "A toolkit for building a data pipeline from your database's binary log stream."
  :url "http://cmerrick.github.io"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :repositories [["conjars" {:url "http://conjars.org/repo"}]]
  :source-paths ["src/clj"]
  :test-paths ["test/clj/"]
  :resource-paths ["resources/"]
  :main plainview.core
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cheshire "5.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [midje "1.6.3"]
                 [org.clojure/java.jdbc "0.3.7"]
                 [com.github.shyiko/mysql-binlog-connector-java "0.2.1"]
                 [mysql/mysql-connector-java "5.1.35"]
                 [rjmetrics "0.1.0"]
                 [clj-http "1.1.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]]
  :plugins [[lein-midje "3.0.1"]])
