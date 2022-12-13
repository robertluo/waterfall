(ns robertluo.waterfall.core
  "Core data structure"
  (:require [manifold.deferred :as d]
            [manifold.stream :as ms] 
            [robertluo.waterfall.util :as util])
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
        strm (ms/stream)]
    (ms/on-closed strm #(.close prod))
    (ms/splice
     strm
     (->> strm 
          (ms/map (fn [{:keys [k v topic partition timestamp]}] 
                    (rmd->map @(.send prod (ProducerRecord. topic partition timestamp k v)))))))))

;--------------------
; Consumer

(util/scala-vo->map
 cr->map
 ConsumerRecord
 [offset partition serializedKeySize serializedValueSize
  timestamp timestampType topic key value headers])

(defn consumer-loop
  [^Consumer consumer mailbox out-sink]
  (loop []
    (let [[k arg :as cmd] @(ms/take! mailbox)]
      (if (= k :close)
        (.close consumer)
        (do
          (case k
            :subscibe (.subscribe consumer arg)
            :poll 
            (do (ms/put-all! out-sink (->> (.poll consumer cmd) (.iterator) (iterator-seq) (map cr->map)))
                (ms/put! mailbox [:poll arg])))
          (recur))))))

(defn consumer
  [nodes group-id topics {:keys [poll-duration] :as conf :or {poll-duration (Duration/ofSeconds 10)}}]
  (let [conr (-> conf
                 (merge {:bootstrap-servers nodes
                         :group-id group-id})
                 util/->config-map
                 (KafkaConsumer. (ByteArrayDeserializer.) (ByteArrayDeserializer.)))
        mailbox (ms/stream)
        out-sink (ms/stream)]
    (ms/put! mailbox [:subscibe topics]) 
    (ms/on-closed out-sink (fn [] @(ms/put! mailbox [:close])))
    (d/future (consumer-loop conr mailbox out-sink))
    (ms/put! mailbox [:poll poll-duration])
    (ms/source-only out-sink)))

(comment
  (def nodes "localhost:9092")
  (def prod (producer nodes {}))
  (ms/put! prod {:topic "test" :k (.getBytes "greeting") :v (.getBytes "Hello, world!")})
  (ms/consume #(println "producer: " %) prod)
  (ms/close! prod)
  (def conr (consumer nodes "test.group" ["test"] {})) 
  (ms/consume #(println "consumer: " %) conr)
  (ms/close! conr)
  )