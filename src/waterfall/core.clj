(ns waterfall.core
  "Core data structure"
  (:require
   [malli.core :as m])
  (:import
   (java.util Map)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig)))

;; Kafka producer/consumer need config map.
;; Instead of using properties in java API, we introduce a concept of definition.

(def schema-registry
  {:server/name     string?
   :server/port     pos-int?
   ::server         [:map :server/name :server/port]
   :cluster/servers [:+ ::server]
   :serde/type      [:enum :string :byte-array]
   :key/serde       :serde/type
   :value/serde     :serde/type
   ::producer       [:map :cluster/servers :key/serde :value/serde]})

(def default-definition
  {:key/serde :byte-array
   :value/serde :byte-array})

(defn def->cfg
  "returns Kafka config map for `definition`"
  [definition] 
  (letfn [(svr->str [{:server/keys [name port]}] (str name ":" port))
          (serde [k] (k {:string "org.apache.kafka.common.serialization.StringSerializer"
                         :byte-array "org.apache.kafka.common.serialization.ByteArraySerializer"}))]
    (let [{servers :cluster/servers key-serde :key/serde val-serde :value/serde}
          (merge default-definition definition)]
      {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG
       (transduce (comp (map svr->str) (interpose ";")) str servers)
       ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG (serde key-serde)
       ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG (serde val-serde)})))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}]
            :key/serde :byte-array})
  (m/explain [:schema {:registry schema-registry} ::producer] clu)
  (def->cfg clu)
  )

(defn producer
  [cluster]
  (KafkaProducer. ^Map (def->cfg cluster)))

(comment
  (producer clu))