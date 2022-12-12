(ns ^:intergration robertluo.waterfall.core-test
  (:require
   [expectations.clojure.test
    :refer [defexpect expect expecting in]] 
   [robertluo.waterfall.core :as sut] 
   [manifold.stream :as ms]))

(def cluster
  {:cluster/servers [#:server{:name "localhost" :port 9092}]})

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
       (.put prod pr true)
       (expect {:topic "test"} (in (.take consumer nil true)))
       (.put prod pr true)
       (expect {:topic "test"} (in @(.take consumer nil false)))
       ))))