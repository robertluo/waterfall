(ns waterfall.core-test
  (:require
   [expectations.clojure.test
    :refer [defexpect in]] 
   [waterfall.core :as sut]))

(defexpect producer-definition
  {:topic/name "greater"}
  (in (sut/producer-definition {:cluster/servers
                                [#:server{:name "localhost" :port 9092}]
                                :topic/name "greater"})))

(defexpect definition->config
  {"bootstrap.servers" "localhost:9092;node2:9091"}
  (sut/definition->config {:cluster/servers
                           [#:server{:name "localhost" :port 9092}
                            #:server{:name "node2" :port 9091}]}))