(ns ^:no-doc robertluo.waterfall.shape
  "Transformation functions of shapes of event data."
  (:refer-clojure :exclude [byte-array])
  (:require
   [clojure.edn :as edn]
   [robertluo.waterfall.util :as util]))

(defn- updater
  "returns a function which apply `f` to both `key` and `value` if it's not nil."
  [f]
  (fn [m]
    (let [update-if (fn [m k f] 
                      (if-not (nil? (get m k))
                        (update m k f)
                        m))]
      (-> m (update-if :key f) (update-if :value f)))))

;;A shape is a conversion definition. It has:
;; - :stage, a number indicate when should it be called, high = serial early/des late
;; - :ser, a function for seriliaztion a value.
;; - :des, a function for deserilization a value.
(defrecord Shape [stage ser des])

(defn shape?
  "predict if `x` is a shape"
  [x]
  (instance? Shape x))

(defn value-only 
  "A shope only concerns the value of Kafka events."
  []
  (->Shape 
   100
   (fn [value] {:value value})
   (fn [m] (:value m))))

(defn key-value 
  "A shape concerns both the key and value of the Kafka event."
  []
  (->Shape
   100
   (fn [[key value]] {:key key :value value})
   (fn [m] [(:key m) (:value m)])))

(defn edn
  "A shape converts string/edn."
  []
  (->Shape
   20
   (updater (fn [data] (pr-str data)))
   (updater (fn [s] (edn/read-string s)))))

(defn byte-array
  "A shape converts byte-array/string."
  []
  (->Shape
   10
   (updater (fn [^String s] (.getBytes s "UTF-8")))
   (updater (fn [^"[B" bs] (String. bs "UTF-8")))))

(defn topic
  "A shape attach topic to record on serilization, do nothing when deserilizing."
  [topic-name]
  (->Shape
   0
   (fn [m] (assoc m :topic topic-name))
   identity))

(defn- fn-selector
  "return functions in a `shapers` data structure in order."
  [shapes f-field]
  (let [f-pair (juxt :stage f-field)]
    (->> shapes
         (map (fn [shape]
                (let [[order target-f] (f-pair shape)]
                  (if (and order target-f)
                    [order target-f]
                    (throw (ex-info "Invalid shape." {:stage order :f target-f})))))) 
         (sort-by first)
         (map second))))

(defn serialize
  "returns a serialization function specified in `ks`"
  [ks]
  (->> (fn-selector ks :ser)
       (apply comp)))

(defn deserialize
  "returns a deserialization function specified in `ks`"
  [ks] 
  (->> (fn-selector ks :des)
       (reverse)
       (apply comp)))

(comment 
  ((serialize [(byte-array) (edn) (value-only)]) {:string "hello"}) 
  ((deserialize [(byte-array) (value-only) (edn)]) {:value (.getBytes "3")})
  ((serialize [(edn) (value-only) (topic "test")]) {:foo "hello"})
  )

(defn nippy 
  "A shape that direct maps data/bytes"
  [] 
  (util/optional-require
   [taoensso.nippy :as nippy]
   (->Shape
    10 
    (updater nippy/freeze)
    (updater nippy/thaw))
   (throw (ClassNotFoundException. "Need com.toensso/nippy library in the classpath"))))

(comment 
  (:stage (nippy))
  )

(defn transit
  "A shape that direct maps data/bytes.
    - `format`: transit supporting format, one of `:msgpack`, `:json`, `:json-verbose`."
  [format]
  (util/optional-require
   [cognitect.transit :as transit]
   (->Shape
    10
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
