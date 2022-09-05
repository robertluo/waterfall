(ns waterfall.core
  (:require
   [malli.core :as m])
  (:import
   (java.util Map)
   (org.apache.kafka.clients.producer Producer KafkaProducer ProducerConfig)))

;;Schema for config
(def ClusterSchema
  [:schema
   {:registry
    {:server/name string?
     :server/port pos-int?
     ::server [:map :server/name :server/port]
     :cluster/servers [:+ ::server]
     :data/shape [:enum :key-value :value]
     ::producer [:map :cluster/servers :data/shape]}}
   ::producer])

(defn cluster->cfg 
  [{:cluster/keys [servers]}]
  (letfn [(svr->str [{:server/keys [name port]}] (str name ":" port))]
    {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG 
     (transduce (comp (map svr->str) (interpose ";")) str servers)}))

(comment
  (def clu {:cluster/servers [#:server{:name "localhost" :port 9092}]})
  (m/explain ClusterSchema clu)
  (cluster->cfg clu)
  )

(defn producer
  [cluster]
  (KafkaProducer. ^Map (cluster->cfg cluster)))

(comment
  (producer clu))