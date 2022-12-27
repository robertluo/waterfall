(ns robertluo.waterfall.shape-test
  (:require
   [robertluo.waterfall.shape :as s]
   [malli.dev]
   [expectations.clojure.test :refer [defexpect expect]]))

(malli.dev/start!)

(defexpect topic-shape
  (expect {:foo "bar" :topic "test"} ((s/serializer [(s/topic (constantly "test"))]) {:foo "bar"})
          ":topic is a high order shape"))

(defexpect round-trip
  (let [data {:foo 'bar :a 3 :b "4"}]
    (doseq [bytes-of [(s/edn) (s/nippy) (s/transit :json) (s/transit :json-verbose) (s/transit :msgpack)]
            shape-of [(s/value-only) (s/key-value (juxt :a identity) second)]
            :let [ks [bytes-of shape-of]]]
      (expect data (->> data ((s/serializer ks)) ((s/deserializer ks)))
              "real world example for edn plain value"))))