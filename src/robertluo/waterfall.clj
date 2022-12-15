(ns robertluo.waterfall
  "API namespace for the library"
  (:require 
   [robertluo.waterfall 
    [core :as core]
    [shape :as shape]]
   [manifold.stream :as ms]))

(defn producer
  "Returns a manifold stream of kafka producer. Can accept map value put onto it,
   and output the putting result. If your do not care about the output, wrap it with `ignore`.
    - `nodes`: bootstrap servers urls, e.g. `localhost:9092`
    - `conf`: optional config `conf`."
  ([nodes]
   (producer nodes {}))
  ([nodes {:as conf}]
   (core/producer nodes conf)))

(defn consumer
  "Returns a manifold source of kafka consumer.
    - `nodes`: bootstrap servers url, e.g `localhost:9092`
    - `group-id`: consumer group id.
    - `topics`: a sequence of topics to listen on. e.g. `[\"test\"]
    - `conf`: an optional config map."
  ([nodes group-id topics]
   (consumer nodes group-id topics {}))
  ([nodes group-id topics {:as conf}]
   (core/consumer nodes group-id topics conf)))

(defn ignore
  "ignore a stream `strm`'s output, return `strm` itself."
  [strm]
  (ms/consume (constantly nil) strm)
  strm)

(defn shaped-source
  "returns a shaped source stream on `src`, will close source if this is closed."
  [src shapes]
  (let [strm (-> (shape/deserialize shapes)
                 (map)
                 (ms/transform src))]
    (ms/on-drained strm #(ms/close! src))
    strm))

(defn shaped-sink
  "returns a shaped sink stream on `sink`"
  [sink shapes]
  (let [strm (ms/stream)]
    (-> (shape/serialize shapes)
        (map)
        (ms/transform strm)
        (ms/connect sink))
    strm))

(comment 
  (def nodes "localhost:9092")
  (def test-consumer (-> (consumer nodes "test.group" ["test"])
                         (shaped-source [(shape/value-only) (shape/edn) (shape/byte-array)])))
  (ms/consume prn test-consumer)

  (def test-producer (-> (ignore (producer nodes))
                         (shaped-sink [(shape/value-only) (shape/edn) (shape/byte-array) (shape/topic "test")]))) 
  (ms/put! test-producer "Hello, world!")
  (ms/close! test-producer)
  (ms/close! test-consumer)
  )