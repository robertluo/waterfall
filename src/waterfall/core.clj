(ns waterfall.core
  "Core data structure"
  (:require
   [manifold.deferred :as defer]
   [manifold.stream :as ms]
   [manifold.stream.core :as sc])
  (:import 
   (java.util Map)
   (org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig Producer ProducerRecord RecordMetadata)
   (org.apache.kafka.clients.consumer KafkaConsumer Consumer)
   (java.time Duration)))

;; Kafka producer/consumer need config map.
;; Instead of using properties in java API, we introduce a concept of definition.

(def schema-registry
  {:non-empty-str    [:string {:min 1}]
   :server/name      :non-empty-str
   :server/port      pos-int?
   ::server          [:map :server/name :server/port]
   :cluster/servers  [:+ ::server]})

(defn def->config
  "returns Kafka config map for `definition`"
  [definition]
  (letfn [(svr->str [{:server/keys [name port]}] (str name ":" port)) 
          (process [[k const f]] (when-let [v (get definition k)] [const (f v)]))]
    (let [processors [[:cluster/servers ProducerConfig/BOOTSTRAP_SERVERS_CONFIG
                       #(transduce (comp (map svr->str) (interpose ";")) str %)]]]
      (into {} (comp (map process) (filter identity)) processors))))

(defn producer
  [cluster-def]
  (KafkaProducer. ^Map (def->config cluster-def) (ByteArraySerializer.) (ByteArraySerializer.)))

(sc/def-sink KafkaProducerSink
  [^Producer producer]
  (isSynchronous [_] false)
  (close [_] (.close producer))
  (description
   [this]
   {:type (.getCanonicalName (class Producer))
    :sink? true
    :closed? (.markClosed this)})
  (put 
   [_ x blocking?]
   (let [{:keys [k v topic partition timestamp]} x
         pr (ProducerRecord. topic partition timestamp k v)    
         rslt (.send producer pr)]
     (if blocking? @rslt rslt)))
  (put
   [this x _ _ _]
   (.put this x false)))

(extend-protocol sc/Sinkable
  Producer
  (to-sink [producer]
    (->KafkaProducerSink producer)))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}]}) 
  (def->config clu)
  (def a (ms/->sink (producer clu)))
  (ms/put! a {:topic "test" :k (.getBytes "hello") :v (.getBytes "world")})
  )

(defn source
  [cluster-def group-id topics]
  (let [config (merge (def->config cluster-def) {"group.id" group-id})
        ^Consumer consumer (KafkaConsumer. config (ByteArrayDeserializer.) (ByteArrayDeserializer.))]
    (.subscribe consumer topics)
    (.poll consumer (Duration/ofSeconds 300))))