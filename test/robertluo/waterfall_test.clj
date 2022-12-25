(ns robertluo.waterfall-test
  #_{:clj-kondo/ignore [:unused-referred-var]}
  (:require [clojure.test :refer [use-fixtures]]
            [expectations.clojure.test :refer [defexpect expect more]]
            [manifold.stream :as ms]
            [malli.dev]
            [robertluo.waterfall :as sut]
            [robertluo.waterfall.shape :as shape])
  (:import (io.github.embeddedkafka EmbeddedKafka EmbeddedKafkaConfig)
           (java.time Duration)))

(malli.dev/start!)

(defn kafka-fixture
  [work]
  (try
    (EmbeddedKafka/start (EmbeddedKafkaConfig/defaultConfig))
    (work)
    (finally
      (EmbeddedKafka/stop))))

(use-fixtures :once kafka-fixture)
(def nodes (str "localhost:" (EmbeddedKafkaConfig/defaultKafkaPort)))

(defexpect round-trip
  #_{:clj-kondo/ignore [:unresolved-var]}
  (let [collector (atom [])
        shapes [(shape/topic (constantly "test")) (shape/edn) (shape/value-only)]]
    (with-open [test-consumer (-> (sut/consumer nodes "test.group" ["test"]
                                                {:poll-duration (Duration/ofSeconds 3)})
                                  (sut/xform-source (map (shape/deserializer shapes))))
                test-producer (-> (sut/producer nodes)
                                  (sut/ignore)
                                  (sut/xform-sink (map (shape/serializer shapes))))]
      (ms/consume #(swap! collector conj %) test-consumer)
      (expect true @(ms/put-all! test-producer (range 1000))
              "Run without exception!") 
      ;;This can not pass on github for unknown reason :-(
      (expect (more not-empty #(every? number? %)) @collector
              "Not sure what received, but at least got some numbers."))))