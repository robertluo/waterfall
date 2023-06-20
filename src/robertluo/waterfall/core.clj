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

(defmacro with-exception-handling
  "Macro to handle exceptions and return them in a controlled manner."
  [ex-msg & body]
  `(try
     (do
       ~@body)
     (catch Exception e#
       (ex-info ~ex-msg {:cause e#}))))

(defn- consumer-actor
  "Manages a Kafka consumer using an actor model to ensure thread safety.
   This function creates a dedicated thread for consuming messages from Kafka,
   and provides a thread-safe way of controlling the consumer through commands.

   Args:
   - consumer: Kafka Consumer instance.
   - out-sink: Manifold stream where consumed messages are put.

   Returns `cmd-self`, a function that accepts following commands:
   - [::close]: Closes the consumer and stops the actor loop.
   - [::subscribe topics]: Subscribes the consumer to given topics.
   - [::seek :beginning]/[::seek :end]: Moves the consumer's offset to the beginning/end.
   - [::resume duration]: Resumes the consumer and commits the offset.
   - [::poll duration]: Polls the consumer for messages.
   
   Unknown commands will raise an exception."
  [^Consumer consumer out-sink]
  (let [mailbox (ms/stream)
        cmd-self (fn [cmd] (ms/put! mailbox cmd)) ; function to post commands to self
        closing? (atom false) ; flag to indicate if the actor is closing
        handle-events (fn [f events] ; function to handle polled events
                        (let [assigns (.assignment consumer)]
                          (when-not (.paused consumer)
                            (.pause consumer assigns)) ; pause consumer when processing events
                          (f events)))
        ensure-sink (fn [f] (when-not (ms/closed? out-sink) (f)))] ; make sure the out-sink is open and call (f)
    (d/future
      (loop []
        (let [cmd @(ms/take! mailbox)]
          (if (= cmd [::close])
            (do (reset! closing? true) ; set closing flag
                (with-exception-handling "on closing consumer"
                  (.close consumer)) ; close consumer
                ::stop) ; stop actor loop
            (do
              (match cmd ; match command
                [::subscribe topics] (.subscribe consumer topics)
                [::seek :beginning] (.seekToBeginning consumer (.assignment consumer))
                [::seek :end] (.seekToEnd consumer (.assignment consumer))
                [::resume duration]
                (do
                  (when (.paused consumer)
                    (.resume consumer (.assignment consumer)))
                  (.commitSync consumer)
                  (cmd-self [::poll duration])) ; resume and poll
                [::poll duration]
                (let [putting-all (fn [events] ; function to handle events and resume
                                    (d/chain (ms/put-all! out-sink events)
                                             #(when % (cmd-self [::resume duration]))))]
                  (ensure-sink
                   (if-let [events (->> (.poll consumer duration) (.iterator) (iterator-seq) (map cr->map) seq)]
                     #(handle-events putting-all events) ; handle events
                     #(cmd-self [::poll duration])))) ; poll again if no events && not closed
                :else (ex-info "unknown command for consumer actor" {:cmd cmd}))
              (when-not @closing?
                (recur))))))) ; continue loop if not closing
    cmd-self))


(defn consumer
  [nodes group-id topics 
   {:keys [poll-duration position]
    :as   conf 
    :or   {poll-duration (Duration/ofSeconds 10)}}]
  (let [conr (-> (dissoc conf :poll-duration :position) ;avoid kafka complaints
                 (merge {:bootstrap-servers nodes
                         :group-id group-id
                         :enable-auto-commit false})
                 util/->config-map
                 (KafkaConsumer. (ByteArrayDeserializer.) (ByteArrayDeserializer.))) 
        out-sink (ms/stream)
        actor (consumer-actor conr out-sink)]
    (actor [::subscribe topics])
    (when position (actor [::seek position]))
    (ms/on-closed out-sink (fn [] @(actor [::close]))) 
    (actor [::poll poll-duration])
    (ms/source-only out-sink)))
