(ns repwrite.coerce
  (:import [com.google.code.or.common.glossary.column
            Int24Column DecimalColumn DoubleColumn
            EnumColumn FloatColumn LongColumn BlobColumn]))

(defmulti coerce class)

(defmethod coerce Int24Column [c] (.getValue c))
(defmethod coerce DecimalColumn [c] (.getValue c))
(defmethod coerce DoubleColumn [c] (.getValue c))
(defmethod coerce EnumColumn [c] (.getValue c))
(defmethod coerce FloatColumn [c] (.getValue c))
(defmethod coerce LongColumn [c] (.getValue c))
(defmethod coerce BlobColumn [c] (String. (.getValue c) "UTF-8"))

(defmethod coerce :default
  [c]
  (.toString c))
