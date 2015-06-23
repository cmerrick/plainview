(ns plainview.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pprint]))

(defn spec [{:keys [host port username password]}]
  {:subprotocol "mysql"
   :subname (str "//" host ":" port "/")
   :user username
   :password password})

(defn databases [db]
  (jdbc/with-db-metadata [m db]
    (jdbc/metadata-result (.getCatalogs m) :row-fn :table_cat)))

(defn tables [db-cfg]
  (->>
   (databases db-cfg)
   (mapcat
    (fn [db]
      (jdbc/with-db-metadata [m db-cfg]
        (jdbc/metadata-result
         (.getTables m db nil nil (into-array String ["TABLE" "VIEW"]))
         :row-fn #(hash-map :database db :table (:table_name %))))))))

(defn columns [db]
  (jdbc/with-db-metadata [m db]
    (->>
     (tables db)
     (mapcat #(jdbc/metadata-result (.getColumns m nil nil % nil)))
                             doall)))

(defn test-query [db]
  (jdbc/query db
           ["select * from journal.journal"]))
