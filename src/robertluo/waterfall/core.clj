(ns robertluo.waterfall.core
  "Core data structure"
  (:require [manifold.deferred :as d]
            [manifold.stream :as ms]
            [manifold.stream.core :as sc]
            [robertluo.waterfall.util :as util])
  (:import (java.time Duration)
           (java.util Map)
           (org.apache.kafka.clients.consumer Consumer ConsumerRecord KafkaConsumer)
           (org.apache.kafka.clients.producer
            KafkaProducer
            Producer
            ProducerConfig
            ProducerRecord
            RecordMetadata)
           (org.apache.kafka.common.serialization ByteArrayDeserializer ByteArraySerializer)))

;; Kafka producer/consumer need config map.
;; Instead of using properties in java API, we introduce a concept of definition.



(util/scala-vo->map
 rmd->map
 RecordMetadata
 [offset partition serializedKeySize
  serializedValueSize timestamp topic])

(defn- produce-record?
  [x]
  (and (map? x)
       (let [{:keys [k v topic partition timestamp]} x]
         (and (not-empty topic) (bytes? k) (bytes? v)
              (or (nil? partition) (pos-int? partition))
              (or (nil? timestamp) (pos-int? timestamp))))))

(defn put*
  [^Producer producer x blocking?]
  (assert (produce-record? x) (str "invalid produce record: " x))
  (let [{:keys [k v topic partition timestamp]} x
        pr (ProducerRecord. topic partition timestamp k v)
        rslt (.send producer pr)]
    (if blocking?
      (rmd->map @rslt)
      (d/chain rslt rmd->map))))

;; Caution: Manifold uses interface for sind/source definition
;; Modify sink/source might need restart REPL
(sc/def-sink KafkaProducerSink
  [^Producer producer]
  (isSynchronous [_] false)
  (close
   [this]
   (.close producer)
   (.markClosed this))
  (description
   [this]
   {:type (.getCanonicalName (class Producer))
    :sink? true
    :closed? (.markClosed this)})
  (put
   [_ x blocking?]
   (put* producer x blocking?))
  (put
   [this x blocking? _ _]
   (.put this x blocking?)))

(defn producer
  [servers & {:as conf}]
  (->KafkaProducerSink
   (let [config (-> conf (merge {:bootstrap-servers servers}) (util/->config-map))]
     (KafkaProducer. ^Map config (ByteArraySerializer.) (ByteArraySerializer.)))))

;--------------------
; Consumer

(util/scala-vo->map
 cr->map
 ConsumerRecord
 [offset partition serializedKeySize serializedValueSize
  timestamp timestampType topic key value headers])

(defn- take*
  "returns a deferred of a KafkaConsumerSource, the implementation of take."
  [^Consumer consumer a-ite duration default-val blocking?]
  (let [f-poll #(.poll consumer duration)]
    (-> (swap! a-ite
            (fn [d-ite]
              (if (or (nil? d-ite)
                      (d/realized? d-ite) ;only re-poll when previous poll finished
                      (and (d/realized? d-ite) (.isDrained @d-ite)))
                (d/chain
                 (if blocking?
                   (d/success-deferred (f-poll))
                   (d/future (f-poll)))
                 #(-> (.iterator %) (ms/->source)))
                d-ite)))
        (d/chain #(.take % default-val blocking?) cr->map))))

(sc/def-source KafkaConsumerSource
  [^Consumer consumer a-ite duration]
  (isSynchronous [_] false)
  (close
   [this]
   (.close consumer)
   (.markDrained this))
  (description
   [_]
   {:type "KafkaConsumerSource"
    :metrics (.metrics consumer)
    :subscription (.subscription consumer)})
  (take
   [_ default-val blocking?]
   (let [d (take* consumer a-ite duration default-val blocking?)]
     (if blocking? @d d))))

(defn consumer
  "retruns a manifold kafka consumer source."
  [nodes group-id topics 
   & {:keys [poll-duration] :as conf
      :or {poll-duration (Duration/ofSeconds 100)}}]
  (let [config (-> conf (merge {:bootstrap-servers nodes
                                :group-id group-id
                                :enable-auto-commit true})
                   (util/->config-map))
        consumer (KafkaConsumer. (util/->config-map config)
                                 (ByteArrayDeserializer.) (ByteArrayDeserializer.))]
    (.subscribe consumer topics)
    (->KafkaConsumerSource consumer (atom nil) poll-duration)))

(comment
  (def nodes "localhost:9092")
  (def prod (producer nodes))
  @(ms/put! prod {:topic "test" :k (.getBytes "hello") :v (.getBytes "world")}) 
  (ms/close! prod)

  (def con (consumer nodes "group.hello" ["test"]))
  (.isDrained con)
  (.take con nil false)
  (def take1 (ms/take! con))
  (if (d/realized? take1) @take1 ::wait)
  (ms/close! con)
  )