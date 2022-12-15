(ns robertluo.waterfall-test
  (:require [clojure.test :refer [use-fixtures]]
            [expectations.clojure.test :refer [defexpect expect]]
            [robertluo.waterfall :as sut]
            [robertluo.waterfall.shape :as shape]
            [manifold.stream :as ms])
  (:import (io.github.embeddedkafka EmbeddedKafka EmbeddedKafkaConfig)))

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
  (with-open [test-consumer (-> (sut/consumer nodes "test.group" ["test"])
                                (sut/shaped-source [(shape/value-only) (shape/edn) (shape/byte-array)]))
              test-producer (-> (sut/producer nodes)
                                (sut/ignore)
                                (sut/shaped-sink [(shape/value-only) (shape/edn) (shape/byte-array) (shape/topic "test")]))]
    (ms/consume prn test-consumer)
    (expect true @(ms/put! test-producer "Hello, world!")
            "Run without exception!")))