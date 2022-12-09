(ns waterfall.core
  "Core data structure"
  (:require
   [manifold.deferred :as defer]
   [manifold.stream :as ms]
   [manifold.utils :as mu]
   [manifold.stream.core :as sc])
  (:import
   (java.util Map)
   (org.apache.kafka.common.serialization ByteArraySerializer ByteArrayDeserializer)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig Producer ProducerRecord RecordMetadata)
   (org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecord)
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

(defn- rmd->map
  [^RecordMetadata rmd]
  (zipmap [:offset :partition :key-size :value-size :timestamp :topic]
          [(.offset rmd) (.partition rmd) (.serializedKeySize rmd)
           (.serializedValueSize rmd) (.timestamp rmd) (.topic rmd)]))

(defn- produce-record?
  [x]
  (and (map? x)
       (let [{:keys [k v topic partition timestamp]} x]
         (and (not-empty topic) (bytes? k) (bytes? v)
              (or (nil? partition) (pos-int? partition))
              (or (nil? timestamp) (pos-int? timestamp))))))

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
   (assert (produce-record? x) (str "invalid produce record: " x))
   (let [{:keys [k v topic partition timestamp]} x
         pr (ProducerRecord. topic partition timestamp k v)    
         rslt (.send producer pr)]
     (if blocking? 
       (rmd->map @rslt) 
       (defer/chain rslt rmd->map))))
  (put
   [this x _ _ _]
   ;;TODO use future cancel to implement timeout?
   (.put this x false)))

(extend-protocol sc/Sinkable
  Producer
  (to-sink [producer]
    (->KafkaProducerSink producer)))

(defn consumer
  ^Consumer [cluster-def group-id topics]
  (let [config (merge (def->config cluster-def) {"group.id" group-id "enable.auto.commit" "true"})
        ^Consumer consumer (KafkaConsumer. config (ByteArrayDeserializer.) (ByteArrayDeserializer.))]
    (.subscribe consumer topics)
    consumer))

(defn- cr->map
  [^ConsumerRecord cr]
  (when cr
    (zipmap
     [:offset :partition :key-size :value-size :timestamp
      :timestamp-type :topic :key :value :headers]
     [(.offset cr)
      (.partition cr)
      (.serializedKeySize cr)
      (.serializedValueSize cr)
      (.timestamp cr)
      (.timestampType cr)
      (.topic cr)
      (.key cr)
      (.value cr)
      (.headers cr)])))

(sc/def-source KafkaConsumerSource
  [^Consumer consumer a-ite]
  (isSynchronous [_] true)
  (close
   [this]
   (.close consumer)
   (.markDrained this))
  (description
   [_]
   {:type (.getCanonicalName (class consumer))})
  (take
   [_ default-val blocking?]
   (-> (swap! a-ite (fn [ite]
                      (if (or (nil? ite) (.isDrained ite))
                        (let [records (.poll consumer (Duration/ofSeconds 1))]
                          (tap> {:cnt (.count records) :records records})
                          (-> (.iterator records) (ms/->source)))
                        ite)))
       (.take default-val blocking?)
       (defer/chain cr->map))))

(extend-protocol sc/Sourceable
  Consumer
  (to-source [consumer]
    (->KafkaConsumerSource consumer (atom nil))))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}]})
  (def prod (ms/->sink (producer clu)))
  (ms/put! prod {:topic "test" :k (.getBytes "hello") :v (.getBytes "world")})
  @*1
  (ms/close! prod)

  (def con (ms/->source (consumer clu "group.hello" ["test"])))
  (.isDrained con)
  (.take con nil false)
  (def take1 (ms/take! con))
  (if (defer/realized? take1) @take1 ::wait)
  (ms/close! con)
  )