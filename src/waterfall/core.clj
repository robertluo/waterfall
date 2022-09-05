(ns waterfall.core
  "Core data structure"
  (:require
   [malli.core :as m])
  (:import
   (java.util Map)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig Producer)))

;; Kafka producer/consumer need config map.
;; Instead of using properties in java API, we introduce a concept of definition.

(def schema-registry
  {:server/name     :string
   :server/port     [:and :int pos?]
   ::server         [:map :server/name :server/port]
   :cluster/servers [:+ ::server]
   :serde/type      [:enum :string :byte-array {:default :byte-array}]
   :key/serde       :serde/type
   :value/serde     :serde/type
   :topic/name      :string
   :group/id        :string
   :producer/compression [:enum :none :gzip :snappy :lz4 :zstd {:default :none}]
   ::producer       [:map
                     :cluster/servers
                     :topic/name
                     [:key/serde {:optional true}]
                     [:value/serde {:optional true}]
                     [:producer/compression {:optional true}]]})

(def default-definition
  {:key/serde :byte-array
   :value/serde :byte-array})

(defn definition->config
  "returns Kafka config map for `definition`"
  [definition] 
  (letfn [(svr->str [{:server/keys [name port]}] (str name ":" port))
          (serde [k] (k {:string "org.apache.kafka.common.serialization.StringSerializer"
                         :byte-array "org.apache.kafka.common.serialization.ByteArraySerializer"}))
          (process [[k const f]] (when-let [v (get definition k)] [const (f v)]))]
    (let [processors [[:cluster/servers ProducerConfig/BOOTSTRAP_SERVERS_CONFIG #(transduce (comp (map svr->str) (interpose ";")) str %)]
                      [:key/serde ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG serde]
                      [:value/serde ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG serde]
                      [:producer/compression ProducerConfig/COMPRESSION_TYPE_CONFIG name]]]
      (into {} (comp (map process) (filter identity)) processors))))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}] 
            :topic/name "greater"})
  (m/explain [:schema {:registry schema-registry} ::producer] clu)
  (definition->config clu)
  )

(defprotocol EventSender
  "A sender can send arbitary clojure data to an event bus (a.k.a Kafka topic)"
  (-send! [sender event] "send an event"))

(defn event->produce-record
  [definition event])

(defrecord KafkaEventSender [definition ^Producer producer]
  EventSender
  (-send!
   [_ event]
   (.send producer (event->produce-record definition event))))

(defn producer
  [definition]
  (KafkaProducer. ^Map (definition->config definition)))

(comment
  (producer clu))
  