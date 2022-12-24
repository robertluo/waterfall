(ns robertluo.waterfall
  "API namespace for the library" 
  (:require 
   [robertluo.waterfall 
    [core :as core]]
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
   [complete conf options](https://kafka.apache.org/33/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html)"
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
   [complete conf options](https://kafka.apache.org/33/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html)"
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

(comment
  (require '[malli.dev]) 
  (malli.dev/start!)
  )