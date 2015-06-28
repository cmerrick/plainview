(ns plainview.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]))

(def ^:private column-map (atom nil))
(def ^:private db-config (atom nil))

(defn- get-all-columns
  []
  (if (nil? @db-config)
    (throw (IllegalStateException. "Database config not set, exiting."))
    (jdbc/query
     @db-config
     [(str "select * from information_schema.columns "
           "order by table_schema, table_name, ordinal_position")]
     :result-set-fn
     (fn [rset]
       (->>
        rset
        (map #(select-keys % [:table_schema
                              :table_name
                              :column_name
                              :column_key]))
        (group-by #(select-keys % [:table_schema
                                   :table_name])))))))

(defn configure!
  [{:keys [host port username password]}]
  (reset! db-config {:subprotocol "mysql"
                     :subname (str "//" host ":" port "/")
                     :user username
                     :password password}))

(defn reset-columns!
  []
  (reset! column-map nil))

(defn get-columns!
  [table-path]
  (when (nil? @column-map)
    (->>
     (reset! column-map (get-all-columns))
     (time)
     (with-out-str)
     (clojure.string/trim)
     (log/info "Pulled column info.")))
  (get @column-map table-path))
