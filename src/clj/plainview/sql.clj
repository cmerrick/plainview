(ns plainview.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pprint]))



(defn spec [{:keys [host port username password]}]
  {:subprotocol "mysql"
   :subname (str "//" host ":" port "/")
   :user username
   :password password})

(defn columns [db]
  (jdbc/query db
    ["select * from information_schema.columns order by table_name, ordinal_position"]
    :result-set-fn (fn [rset]
                     (->>
                      rset
                      (map #(select-keys % [:table_catalog :table_name :column_name]))
                      (group-by :table_name)
                      (group-by :table_catalog)))))
