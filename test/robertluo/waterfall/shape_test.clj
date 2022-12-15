(ns robertluo.waterfall.shape-test
  (:require
   [robertluo.waterfall.shape :as s]
   [expectations.clojure.test :refer [defexpect expect]]))

(defexpect serialize
  (expect {:value "{:foo bar}" :key nil} ((s/serialize [(s/value-only) (s/edn)]) {:foo 'bar})
          "serialize value-only and edn")
  (expect {:key "3" :value "5"} ((s/serialize [(s/edn) (s/key-value)]) [3 5])
          "serialize key-value and edn"))

(defexpect deserialize
  (expect {:foo 'bar} ((s/deserialize [(s/value-only) (s/edn)]) {:value "{:foo bar}"})
          "deserilize value-only edn, copied the value/expection")
  (expect [3 5] ((s/deserialize [(s/key-value) (s/edn)]) {:key "3" :value "5"})
          "deserilize key-value and edn"))

(defexpect topic-shape
  (expect {:foo "bar" :topic "test"} ((s/serialize [(s/topic "test")]) {:foo "bar"})
          ":topic is a high order shape"))

(defexpect round-trip
  (let [ks [(s/value-only) (s/edn) (s/byte-array)]
        data {:foo 'bar :a 3 :b "4"}]
    (expect data (->> data ((s/serialize ks)) ((s/deserialize ks)))
            "real world example for edn plain value")))