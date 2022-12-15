(ns robertluo.waterfall.shape-test
  (:require
   [robertluo.waterfall.shape :as sut]
   [expectations.clojure.test :refer [defexpect expect]]))

(defexpect serialize
  (expect {:value "{:foo bar}" :key nil} ((sut/serialize [:value-only :edn]) {:foo 'bar})
          "serialize value-only and edn")
  (expect {:key "3" :value "5"} ((sut/serialize [:edn :key-value]) [3 5])
          "serialize key-value and edn"))

(defexpect deserialize
  (expect {:foo 'bar} ((sut/deserialize [:value-only :edn]) {:value "{:foo bar}"})
          "deserilize value-only edn, copied the value/expection")
  (expect [3 5] ((sut/deserialize [:key-value :edn]) {:key "3" :value "5"})
          "deserilize :key-value and :edn"))

(defexpect topic-shape
  (expect {:foo "bar" :topic "test"} ((sut/serialize [[:topic "test"]]) {:foo "bar"})
          ":topic is a high order shape")
  (expect clojure.lang.ExceptionInfo ((sut/deserialize [[:topic "test"]]) 'anything)
          ":topic can not be used on deserialization"))

(defexpect round-trip
  (let [ks [:value-only :edn :byte-array]
        data {:foo 'bar :a 3 :b "4"}]
    (expect data (->> data ((sut/serialize ks)) ((sut/deserialize ks)))
            "real world example for edn plain value")))