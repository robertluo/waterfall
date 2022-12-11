(ns waterfall.core-test
  (:require
   [expectations.clojure.test
    :refer [defexpect expect expecting in]] 
   [waterfall.core :as sut]
   [clojure.test :refer [use-fixtures]]
   [manifold.stream :as ms])
  (:import
   (io.github.embeddedkafka EmbeddedKafka EmbeddedKafkaConfig)))

(def cluster
  {:cluster/servers [#:server{:name "localhost" :port 9092#_(EmbeddedKafkaConfig/defaultKafkaPort)}]})

(defn with-kafka
  [work]
  (try
    (EmbeddedKafka/start (EmbeddedKafkaConfig/defaultConfig))
    (work)
    (finally
      (EmbeddedKafka/stop))))

;;This is really slow, use it only neccessary!
;(use-fixtures :once with-kafka)

(defexpect round-trip
  (with-open [prod (ms/->sink (sut/producer cluster))
              consumer (ms/->source (sut/consumer cluster "test.group" ["test"]))]
    (let [pr {:topic "test" :k (.getBytes "hello") :v (.getBytes "world")}] 
      (expecting
       "producer conform protocol"
       (expect false (.isSynchronous prod))
       (expect {:sink? true} (in (.description prod)))
       (expect {:topic "test"} (in (.put prod pr true)))
       (expect {:topic "test"} (in @(.put prod pr false))))
      (expecting
       "consumer conform protocol"
       (expect false (.isSynchronous consumer))
       (expect {:subscription #{"test"}} (in (.description consumer)))
       (ms/put! prod pr)
       (expect {} (in @(ms/take! consumer)))
       ))))