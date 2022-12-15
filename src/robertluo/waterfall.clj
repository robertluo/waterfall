(ns robertluo.waterfall
  "API namespace for the library"
  (:require 
   [robertluo.waterfall.core :as core]))

(defn producer
  "Returns a manifold stream of kafka producer. Can accept map value put onto it,
   and output the putting result.
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

(comment
  (require '[manifold.stream :as ms])
  (def nodes "localhost:9092")
  (def conr (consumer nodes "test.group" ["test"]))
  (ms/consume prn conr)

  (def prod (producer nodes))
  (ms/consume (constantly nil) prod)
  (ms/put! prod {:topic "test" :k (.getBytes "greeting") :v (.getBytes "Hello, world!")})
  (ms/close! prod)
  (ms/close! conr)
  )