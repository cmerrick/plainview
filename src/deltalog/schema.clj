(ns deltalog.schema)

(defn- generic-schema  ; documentation for reference only
  []
  {:id 1
   :timestamp (long 123456789)
   :is-delete 0
   :data [1, "actual-name", "actual-email", 1]})

(def user [:id, :name, :email, :state])
(def cart [:id, :created_at, :user_id, :state, :item_count])
(def chart [:id, :name, :metric, :filters])

(defn get-index [schema field]
  (.indexOf schema field))
