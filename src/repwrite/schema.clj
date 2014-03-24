(ns repwrite.schema
  (:require [abracad.avro :as avro])
  (:import [org.apache.avro Schema$Parser]))

(def user [:id, :name, :email, :state])
(def cart [:id, :created_at, :user_id, :state, :item_count])
(def chart [:id, :name, :metric, :filters])

(defn get-index [schema field]
  (.indexOf schema field))

(def user-schema
  (avro/parse-schema
   {:type :record
    :name "user"
    :fields [{:name "timestamp" :type :long}
             {:name "delete" :type :boolean}
             {:name "id" :type :int}
             {:name "name" :type :string}
             {:name "email" :type :string}
             {:name "state" :type :int}]}))

(defn tableid->schema [table-id]
  (case table-id
    323 user-schema

    user-schema))
