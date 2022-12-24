(ns robertluo.waterfall.shape
  "Transformation functions of shapes of event data."
  (:require
   [clojure.edn :as edn]
   [robertluo.waterfall.util :as util]))

;;A shape is a conversion definition. It has:
;; - :ser, a function for seriliaztion a value.
;; - :des, a function for deserilization a value.
(defrecord Shape [ser des])

(defn shape?
  "predict if `x` is a shape"
  {:malli/schema
   [:=> [:cat :any] :boolean]}
  [x]
  (instance? Shape x))

(def schema
  "schema for shapes"
  {:registry
   {::shape [:fn shape?]}})

(defn value-only
  "A shape concerns just value of the Kafka event."
  {:malli/schema
   [:=> schema :cat ::shape]}
  []
  (->Shape 
   (fn [v] {:value v})
   (fn [m] (:value m))))

(defn key-value 
  "A shape concerns both the key and value of the Kafka event.
   When serializing a value, `f` will apply to the value, the return will
   be the key of the event.
   When deserializing a value, optional function `g` will apply to the
   key and value pari of the event. Default is `identity`."
  {:malli/schema
   [:function schema
    [:=> [:cat [:=> [:cat :any] [:tuple :any :any]]] ::shape]
    [:=> [:cat 
          [:=> [:cat :any] [:tuple :any :any]]
          [:=> [:cat :map] :any]] ::shape]]}
  ([f]
   (key-value f identity))
  ([f g]
   (->Shape 
    (fn [value] 
      (let [[k v] (f value)] {:key k :value v}))
    (fn [{:keys [key value]}]
      (g [key value])))))

(defn- updater
  "returns a function which apply `f` to both `key` and `value` if it's not nil."
  [f]
  (fn [m]
    (let [update-if (fn [m k f]
                      (if-not (nil? (get m k))
                        (update m k f)
                        m))]
      (-> m (update-if :key f) (update-if :value f)))))

(defn edn
  "A shape converts string/edn."
  {:malli/schema
   [:=> schema [:cat] ::shape]}
  []
  (->Shape
   (updater (fn [data] (-> ^String (pr-str data) (.getBytes "UTF-8"))))
   (updater (fn [^"[B" bs] (-> (String. bs "UTF-8") (edn/read-string))))))

(defn topic
  "A shape attach topic to record on serilization, do nothing when deserilizing.
   - `f-topic`: function take data (records with `:key` and `:value`), returns
      a string of topic name."
  {:malli/schema
   [:=> schema [:cat [:=> [:cat :map] :any]] ::shape]}
  [f-topic]
  (->Shape
   (fn [m] (assoc m :topic (f-topic m)))
   identity))

(defn serializer
  "returns a serializer function which composes `shapes`, make sure to put the
   last step (like `value-only`) to the last.
   e.g. [(edn) (value-only)]"
  {:malli/schema
   [:=> schema [:cat [:sequential ::shape]] [:=> [:cat :any] bytes?]]}
  [shapes]
  (->> (map :ser shapes) (apply comp)))

(defn deserializer
  "returns a deserializer function which composes `shapes`, will be called in the
   reverse order of `serializer`, so the `shapes` argument can be shared.
   e.g. [(edn) (value-only)]"
  {:malli/schema
   [:=> schema [:cat [:sequential ::shape]] [:=> [:cat bytes?] :any]]}
  [shapes]
  (->> (map :des shapes) (reverse) (apply comp)))

(comment
  (def shapes [(edn) (key-value identity)])
  ((serializer shapes) [:foo "bar"])
  ((deserializer shapes) *1)
  (shape? (edn))
  )

(defn nippy 
  "A shape that direct maps data/bytes"
  {:malli/schema
   [:=> schema [:cat] ::shape]}
  [] 
  (util/optional-require
   [taoensso.nippy :as nippy]
   (->Shape
    (updater nippy/freeze)
    (updater nippy/thaw))
   (throw (ClassNotFoundException. "Need com.toensso/nippy library in the classpath"))))

(defn transit
  "A shape that direct maps data/bytes.
    - `format`: transit supporting format, one of `:msgpack`, `:json`, `:json-verbose`."
  {:malli/schema
   [:=> schema [:cat [:enum :msgpack :json :json-verbose]] ::shape]}
  [format]
  (util/optional-require
   [cognitect.transit :as transit]
   (->Shape
    (fn [m]
      (with-open [^java.io.ByteArrayOutputStream out (java.io.ByteArrayOutputStream.)]
        (let [wtr (transit/writer out format)]
          (transit/write wtr m)
          (.toByteArray out))))
    (fn [bs]
      (with-open [^java.io.ByteArrayInputStream in (java.io.ByteArrayInputStream. bs)]
        (let [rdr (transit/reader in format)]
          (transit/read rdr)))))
   (throw (ClassNotFoundException. "Need com.cognitect/transit-clj library in the classpath"))))
