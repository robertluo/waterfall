;;# Easier API
(ns easy
  (:require [robertluo.waterfall :as wf]
            [robertluo.waterfall.shape :as shape]))

;; Suppose you just want to use Kafka right away, do not want to learn
;; too much, and do not want to use manifold/kafka API, do not want know
;; which namespaces to require, waterfall has a single function for you.

;; ## One map to rule them all

;; Suppose all you want is to publish message to a kafka topic:
(def cluster
  (wf/kafka-cluster
   {::wf/nodes "localhost:9092"
    ::wf/shapes [(shape/topic (constantly "sentence")) (shape/edn) (shape/value-only)]}))

;; Declare a var (fn) allow us to put message.
(def put! (::wf/put! cluster))

;; Put clojure data onto it when needed!
(put! {:words "Hello, world!"})

;; When you have done using it, just close the cluster and it releases all resources.
(.close cluster)

;;If you want to consumer messages from kafka topics, a cluster need a little more configuration:
(def consuming-cluster
  (wf/kafka-cluster
   {::wf/nodes "localhost:9092"
    ::wf/shapes [(shape/topic (constantly "sentence")) (shape/edn) (shape/value-only)]
    ::wf/consumer-config {:position :beginning}
    ::wf/group-id "tester1"
    ::wf/topics ["sentence"]}))

;; Just refer to its `::wf/consume` key
(def consume (::wf/consume consuming-cluster))

;; Then register you interest on it, here, we just print every message out:
(consume println)

;;When done, close it. You might have to wait up to 10 seconds for it.
(.close consuming-cluster)

;;## where the power come from
;; This easy integration are powered by [fun-map](https://github.com/robertluo/fun-map),
;; so make suer you put it in your dependencies.