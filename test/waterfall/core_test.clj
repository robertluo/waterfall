(ns waterfall.core-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [waterfall.core :as sut]))

(deftest definition->config
  (testing ":cluster/servers will expand to bootstrap.servers"
    (is (= {"bootstrap.servers" "localhost:9092;node2:9091"} 
           (sut/definition->config {:cluster/servers
                                    [#:server{:name "localhost" :port 9092}
                                     #:server{:name "node2" :port 9091}]})))))