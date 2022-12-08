(ns waterfall.core
  "Core data structure"
  (:require
   [manifold.deferred :as defer])
  (:import 
   (java.util Map)
   (org.apache.kafka.common.serialization ByteArraySerializer)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig Producer ProducerRecord RecordMetadata)))

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

(defn sink
  [cluster-def]
  (let [^Producer producer (KafkaProducer. ^Map (def->config cluster-def) (ByteArraySerializer.) (ByteArraySerializer.))
        rmd->map (fn [^RecordMetadata rmd] (zipmap [:offset :partition :key-size :value-size :timestamp :topic] [ (.offset rmd) (.partition rmd) (.serializedKeySize rmd) (.serializedValueSize rmd) (.timestamp rmd) (.topic rmd)]))]
    (fn [{:keys [k v topic partition timestamp]}]
      (let [pr (ProducerRecord. topic partition timestamp k v)
            rslt (defer/->deferred (.send producer pr))]
        (defer/chain rslt rmd->map)))))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}]}) 
  (def->config clu)
  (def a (sink clu))
  (def rslt (a {:topic "test" :k (.getBytes "name") :v (.getBytes "luotian")}))
  @rslt
  )
