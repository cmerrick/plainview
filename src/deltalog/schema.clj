(ns deltalog.schema
  (:import [org.apache.avro Schema$Parser]))

(defn- generic-schema  ; documentation for reference only
  []
  {:id 1
   :timestamp (long 123456789)
   :is_delete 0
   :data [1, "actual-name", "actual-email", 1]})

(def user [:id, :name, :email, :state])
(def cart [:id, :created_at, :user_id, :state, :item_count])
(def chart [:id, :name, :metric, :filters])

(defn get-index [schema field]
  (.indexOf schema field))

(defn avro-schema []
  (let [parser (Schema$Parser.)]
    (.parse parser
            "{
     \"type\": \"record\",
         \"name\": \"test\",
             \"fields\" : [
                               {\"name\": \"id\", \"type\": \"int\"},
                                   {\"name\": \"timestamp\", \"type\": \"int\"},
                                   {\"name\": \"is_delete\", \"type\": \"int\"},
                                   {\"name\": \"data\", \"type\": \"string\"}]}"          )))
