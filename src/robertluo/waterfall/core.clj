(ns ^:no-doc robertluo.waterfall.core
  "Core data structure"
  (:require [manifold.deferred :as d]
            [manifold.stream :as ms] 
            [robertluo.waterfall.util :as util]
            [clojure.core.match :refer [match]])
  (:import (java.time Duration)
           (java.util Map)
           (org.apache.kafka.clients.consumer Consumer ConsumerRecord KafkaConsumer)
           (org.apache.kafka.clients.producer
            KafkaProducer
            Producer 
            ProducerRecord
            RecordMetadata)
           (org.apache.kafka.common.serialization ByteArrayDeserializer ByteArraySerializer)))

;----------------------------
; producer

(util/scala-vo->map
 rmd->map
 RecordMetadata
 [offset partition serializedKeySize
  serializedValueSize timestamp topic])

(defn producer
  [servers conf]
  (let [config (-> conf (merge {:bootstrap-servers servers}) (util/->config-map))
        ^Producer prod (KafkaProducer. ^Map config (ByteArraySerializer.) (ByteArraySerializer.))
        strm (ms/stream)
        sending (fn [x]
                  (let [{:keys [key value topic partition timestamp]} x]
                    (rmd->map @(.send prod (ProducerRecord. topic partition timestamp key value)))))]
    (ms/on-closed strm #(.close prod)) 
    (ms/splice strm (ms/map sending strm))))

;--------------------
; Consumer

(util/scala-vo->map
 cr->map
 ConsumerRecord
 [offset partition serializedKeySize serializedValueSize
  timestamp timestampType topic key value headers])

(defn- consumer-actor
  "Kafka consumer is not thread safe, using actor model will limit its access in a single thread."
  [^Consumer consumer out-sink]
  (let [mailbox (ms/stream)
        cmd-self (fn [cmd] (ms/put! mailbox cmd))]
    (d/future
      (loop []
        (when-not
         (= ::stop
            (let [cmd @(ms/take! mailbox)]
              (tap> cmd)
              (match cmd
                [:close] (do (.close consumer) ::stop)
                [:subscibe topics] (.subscribe consumer topics)
                [:seek :beginning] (.seekToBeginning consumer (.assignment consumer))
                [:seek :end] (.seekToEnd consumer (.assignment consumer))
                [:resume assigns duration]
                (do (when (.paused consumer)
                      (.resume consumer assigns))
                    (.commitSync consumer)
                    (cmd-self [:poll duration]))
                [:poll duration]
                (let [f-poll #(->> (.poll consumer duration) (.iterator) (iterator-seq) (map cr->map))
                      assigns (.assignment consumer)]
                  (when-not (.paused consumer)
                    (.pause consumer assigns))
                  (d/chain (ms/put-all! out-sink (f-poll))
                           (fn [rslt]
                             (when rslt
                               (cmd-self [:resume assigns duration])))))
                :else (ex-info "unknown command for consumer actor" {:cmd cmd}))) )
          (recur))))
    cmd-self))

(defn consumer
  [nodes group-id topics 
   {:keys [poll-duration position] :as conf 
    :or {poll-duration (Duration/ofSeconds 10)}}]
  (let [conr (-> conf
                 (merge {:bootstrap-servers nodes
                         :group-id group-id
                         :enable-auto-commit false})
                 util/->config-map
                 (KafkaConsumer. (ByteArrayDeserializer.) (ByteArrayDeserializer.))) 
        out-sink (ms/stream)
        actor (consumer-actor conr out-sink)]
    (actor [:subscibe topics])
    (when position (actor [:seek position]))
    (ms/on-closed out-sink (fn [] @(actor [:close]))) 
    (actor [:poll poll-duration])
    (ms/source-only out-sink)))

(comment
  (def nodes "localhost:9092")
  (def prod (producer nodes {}))
  (ms/put! prod {:topic "test" :k (.getBytes "greeting") :v (.getBytes "Hello, world!")})
  (ms/consume #(println "producer: " %) prod)
  (ms/close! prod)
  (def conr (consumer nodes "test.group" ["test"] {:position :beginning})) 
  (ms/consume #(println "consumer: " %) conr)
  (ms/close! conr) 
  )