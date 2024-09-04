(ns robertluo.waterfall
  "API namespace for the library"
  (:require 
   [robertluo.waterfall
    [core :as core]
    [util :as util]
    [shape :as shape]]
   [manifold.stream :as ms]))

(def schema
  "Schema for waterfall"
  {:registry
   {::nodes          [:re #"(.*?:\d{2,5};?)+"]
    ::producer-config [:map]
    ::consumer-config [:map
                       [:position {:optional true} [:enum :beginning :end]]
                       [:poll-duration {:optional true} [:fn #(instance? java.time.Duration %)]]]
    ::stream [:fn ms/stream?]
    ::source [:fn ms/source?]
    ::sink [:fn ms/sink?]}})

(defn producer
  "Returns a manifold stream of kafka producer. Can accept map value put onto it,
   and output the putting result. If your do not care about the output, wrap it with `ignore`.
    - `nodes`: bootstrap servers urls, e.g. `localhost:9092`
    - `conf`: optional config `conf`. 
   [other conf options](https://kafka.apache.org/33/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html)"
  {:malli/schema 
   [:function schema
    [:=> [:cat ::nodes] ::stream]
    [:=> [:cat ::nodes ::producer-config] ::stream]]}
  ([nodes]
   (producer nodes {}))
  ([nodes {:as conf}]
   (core/producer nodes conf)))

(defn consumer
  "Returns a manifold source of kafka consumer.
    - `nodes`: bootstrap servers url, e.g `localhost:9092`
    - `group-id`: consumer group id.
    - `topics`: a sequence of topics to listen on. e.g. `[\"test\"]
    - `conf`: an optional config map.
   [other conf options](https://kafka.apache.org/33/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html)"
  {:malli/schema
   [:function schema
    [:=> [:cat ::nodes :string [:vector :string]] ::source]
    [:=> [:cat ::nodes :string [:vector :string] ::consumer-config] ::source]]}
  ([nodes group-id topics]
   (consumer nodes group-id topics {}))
  ([nodes group-id topics {:as conf}]
   (core/consumer nodes group-id topics conf)))

(defn ignore
  "ignore a stream `strm`'s output, return `strm` itself."
  {:malli/schema 
   [:=> schema [:cat ::stream] ::stream]}
  [strm]
  (ms/consume (constantly nil) strm)
  strm)

(defn xform-source
  "returns a source stream on `src` transforming event from `src` using transducer `xform`.
   will close source if this is drained.
   - `src`: source stream.
   - `xform`: a transducer for transforming data from `src`"
  {:malli/schema
   [:=> schema [:cat ::stream fn?] ::source]}
  [src xform]
  (let [strm (ms/transform xform src)]
    (ms/on-drained strm #(ms/close! src))
    (ms/source-only strm)))

(defn xform-sink
  "returns a sink stream on `sink` stream, all event put on it will be transformed using
   transducer `xform` before pass to `sink`.
   - `sink`: sink stream.
   - `xform`: a transducer transforms events, then put to `sink`"
  {:malli/schema
   [:=> schema [:cat ::stream fn?] ::stream]}
  [sink xform]
  (let [strm (ms/stream)]
    (-> (ms/transform xform strm) (ms/connect sink))
    strm))

(defn kafka-cluster
  "returns a convient life-cycle-map of a kafka cluster. It requires:
    - `::nodes` kafka bootstrap servers
    - `::shapes` shape of data, example: `[(shape/topic (constantly \"sentence\"))(shape/edn)(shape/value-only)]`
   
   If you just use it to publish message,
    - optional `::producer-config` can specify additional kafka producer configuration.
   
   If you want to consume from topics:
    - `::topics` the topic you want to subscribe to. example: `[\"sentence\"]`
    - `::group-id` the group id for the message consumer
    - optional `::source-xform` is a transducer to process message before consuming
    - optional `::consumer-config` can specify additional kafka consumer configuration. With additions:
      - `:position` either `:beginning` `:end`, none for commited position (default)
      - `:poll-duration` for how long the consumer poll returns, is a Duration value, default 10 seconds
      - `:commit-strategy` one of `:sync`, `:async`, `:auto`, default `:sync`
   
   The returned map has different level of key-values let you use:
    - Highest level, no additional knowledge:
      - For consumer: `::consume` a function, a one-arity (each message) function as its arg, returns `nil`.
      - For producer: 
        -`::put!` a function with a message as its arg. e.g. `((::put return-map) {:a 3})`
        -`::put-all!` a function with message sequence as its arg
    - Mid level, if you need access of underlying manifold sink/source.
      - `::source` a manifold source.
      - `::sink` a manifold sink.
    - Lowest level, if you want to access kafka directly:
      - `::consumer` a Kafka consumer.
      - `::producer` a Kafka message producer.
   "
  {:malli/schema
   [:=> schema [:cat [:map {:closed true}
                      [::nodes ::nodes]
                      [::shapes [:vector [:fn shape/shape?]]]
                      [::consumer-config {:optional true} ::consumer-config]
                      [::producer-config {:optional true} ::producer-config]
                      [::group-id {:optional true} :string]
                      [::topics {:optional true} [:vector :string]]
                      [::source-xform {:optional true} fn?]]]
    :map]}
  [kafka-conf-map]
  (util/optional-require
   [robertluo.fun-map :as fm :refer [fw fnk]]
   (merge
    (fm/life-cycle-map
     {::producer (fnk [::nodes ::producer-config]
                      (let [prod (producer nodes (or producer-config {}))]
                        (fm/closeable prod #(ms/close! prod))))
      ::consumer (fnk [::nodes ::group-id ::topics ::consumer-config]
                      (assert (and group-id topics) "Has to provide group-id and topics")
                      (let [cmer (consumer nodes group-id topics (or consumer-config {}))]
                        (fm/closeable cmer #(ms/close! cmer))))
      ::sink (fnk [::producer ::shapes]
                 (-> producer ignore (xform-sink (comp (map (shape/serializer shapes))))))
      ::source (fnk [::consumer ::shapes ::source-xform]
                    (->> (comp (map (shape/deserializer shapes)) (or source-xform (map identity)))
                         (xform-source consumer)))
      ::put! (fnk [::sink] (partial ms/put! sink))
      ::put-all! (fnk [::sink] (partial ms/put-all! sink))
      ::consume (fnk [::source] #(ms/consume % source))})
    kafka-conf-map)
   (throw (ClassNotFoundException. "Need io.github.robertluo/fun-map library in the classpath"))))

(comment 
  (require '[malli.dev]) 
  (malli.dev/start!)
  (malli.dev/stop!)
  (def clu
    (kafka-cluster 
     {::nodes "localhost:9092"
      ::shapes [(shape/topic (constantly "sentence")) (shape/edn) (shape/value-only)]
      ::consumer-config {:position :beginning}
      ::group-id "tester1"
      ::topics ["sentence"]
      ::source-xform (map identity)}))
  (def put! (::put! clu))
  (put! "Hello, world")
  (def consume (::consume clu))
  (consume println) 
  (.close clu) 
  )