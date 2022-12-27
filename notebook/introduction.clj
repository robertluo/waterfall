;; # Hello, Waterfall! A Word counting example.
(ns introduction
  (:require [robertluo.waterfall :as wf])
  (:import (java.time Duration)))

;; Waterfall is a library for Clojure programmers to use Apache Kafka. In order to run
;; this introduction, you need to run a local Kafka server on the default port (9092).
(def nodes "localhost:9092")
;; Suppose we want to count words in real time, for every sentence, we tokenize it first:
(require '[clojure.string :as str])
(defn tokenize [line]
  (-> line (str/trim) (str/split #"\s+")))
;;Let's test it:
(tokenize "Hello world")
;;## Transducer
;; Let's write a transducer first:
(defn xf-running-total
  []
  (fn [rf]
    (let [state (volatile! {})]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result m]
         (let [next (vswap! state (partial merge-with (fnil + 0)) m)]
           (rf result next)))))))
(def word-count-xform 
  (comp (map tokenize)
        (map frequencies)
        (xf-running-total)))

;; Test it on plain old Clojure sequences:
(sequence word-count-xform ["Hello world" "Hello Robert"])

;;## Bring Kafka
;;`Shape` is the way Waterfall to serialize/deserialize.
;; They are just normal function pairs.
(require '[robertluo.waterfall.shape :as shape]) 

;;Our example only concerns topic "sentence", its data is edn, and we do not care about
;;Kafka key (nil key).
(def shapes [(shape/topic (constantly "sentence"))(shape/edn)(shape/value-only)])

;; ## Where are our sentence come from?
;; Use `wf/consumer` to listen on a Kafka topic/topics and using our xform to connect.
(def sentence-source 
  (-> (wf/consumer nodes "test.group" ["sentence"] 
                   {:position :beginning, :poll-duration (Duration/ofSeconds 3)})
      (wf/xform-source (comp (map (shape/deserializer shapes)) word-count-xform))))

;;Waterfall using manifold stream:
(require '[manifold.stream :as ms])
;;A source must have an output in order not blocking the source. Let's store the running
;;updating state into an atom.
(def receiver-atom (atom []))
(ms/consume #(reset! receiver-atom %) sentence-source)

;;## Feed sentences into topic
;;A producer can send event to many topic regarding the content of event,
;;in this example, we specified `(constantly "sentence")`. A producer is both a sink/source,
;;however, most of time, you may choose to `ignore` its sending results.
(def line-feeder
  (-> (wf/producer nodes)
      (wf/ignore)
      (wf/xform-sink (comp (map (shape/serializer shapes))))))

;;Feed some sentences into it:
(ms/put-all! line-feeder ["A quick fox jumped over a lazy dog"
                          "hello world"
                          "this is a good example"])

;;## Close them:
(ms/close! line-feeder)
;;This could be really slow, because it need to wait for last polling.
(ms/close! sentence-source)
;;## Checking result
;;Let's check receiver atom: 
@receiver-atom
