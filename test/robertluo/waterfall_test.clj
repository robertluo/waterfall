(ns robertluo.waterfall-test
  (:require
   [robertluo.waterfall :as sut]
   [clojure.test :refer [use-fixtures]]
   [expectations.clojure.test :refer [defexpect expecting expect in]]
   [manifold.stream :as ms])
  (:import
   (io.github.embeddedkafka EmbeddedKafka EmbeddedKafkaConfig)))

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
  (with-open [prod (sut/producer nodes)
        ;conr (sut/consumer nodes "test.group" ["test"] {:position :beginning})
              ]
    (ms/connect (repeat 3 {:topic "test" :v (.getBytes "Hello")}) prod) 
    (expect 2 (-> (ms/stream->seq prod 100) count))
    ;(expect 2 (-> (ms/stream->seq conr 100) count))
    ))