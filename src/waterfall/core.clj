(ns waterfall.core
  "Core data structure"
  (:require
   [malli.core :as m]
   [manifold.deferred :as defer])
  (:import
   (java.util Map)
   (org.apache.kafka.clients.producer KafkaProducer ProducerConfig Producer ProducerRecord)))

;; Kafka producer/consumer need config map.
;; Instead of using properties in java API, we introduce a concept of definition.

(def schema-registry
  {:non-empty-str    [:string {:min 1}]
   :server/name      :non-empty-str
   :server/port      pos-int?
   ::server          [:map :server/name :server/port]
   :cluster/servers  [:+ ::server]

   :serde/type       [:enum :string :byte-array {:default :byte-array}]
   :key/serde        :serde/type
   :value/serde      :serde/type
   :event/data       :any
   :event/key-func   [:=> [:cat :event/data] :event/data]
   :event/value-func [:=> [:cat :event/data] :event/data]

   :topic/name      :non-empty-string
   ::producer       [:map
                     :cluster/servers
                     :topic/name
                     [:key/serde {:optional true}]
                     [:value/serde {:optional true}]
                     [:event/key-func {:optional true}]
                     [:event/value-func {:optional true}]
                     [:producer/compression {:optional true}]]})

(def default-definition
  {:key/serde        :byte-array
   :value/serde      :byte-array
   :event/key-func   (constantly nil)
   :event/value-func identity})

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
  (definition->config clu))

(defprotocol EventSender
  "A sender can send arbitary clojure data to an event bus (a.k.a Kafka topic)"
  (-send! [sender event] "send an event"))

(defn event->producer-record
  [definition event]
  (let [{^String topic :topic/name
         k-serde :key/serde v-serde :value/serde
         key-func :event/key-func value-func :event/value-func} definition
        f-ser (fn [ser-type]
                (case ser-type
                  :string pr-str
                  :byte-array (comp #(.getBytes ^String % "UTF-8") pr-str)))
        k (-> event key-func ((f-ser k-serde)))
        v (-> event value-func ((f-ser v-serde)))]
    (ProducerRecord. topic k v)))

(defrecord KafkaEventSender [definition ^Producer producer]
  EventSender
  (-send!
    [_ event]
    (.send producer (event->producer-record definition event))))

(defn kafka-event-sender
  [definition]
  (let [definition (merge default-definition definition)
        producer (KafkaProducer. ^Map (definition->config definition))]
    (->KafkaEventSender definition producer)))

(comment
  (require '[manifold.deferred :as defer])
  (def sender (kafka-event-sender clu))
  (defer/chain (-send! sender "hello, world again!") (comp println str))
  *1
  )
