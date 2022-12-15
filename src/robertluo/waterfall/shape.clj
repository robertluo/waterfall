(ns robertluo.waterfall.shape
  "Transformation functions of shapes of event data."
  (:require [clojure.edn :as edn]))

(defn- updater
  "returns a function which apply `f` to both `key` and `value` if it's not nil."
  [f]
  (fn [m]
    (let [f (fn [v] (when v (f v)))]
      (-> m (update :key f) (update :value f)))))

(def default-shapers
  "default shapers table.
   A shaper is a map contains 3 keys:
    - `:ser`: a function to serialize value.
    - `:des`: a function to deserialize value.
    - `:stage`: a number indicate the order of the execution."
  {:value-only 
   {:doc "Only concerns the value of the Kafka event"
    :ser (fn [value] {:value value})
    :des (fn [m] (:value m))
    :stage 100}
   :key-value 
   {:doc "Concerns both the key and value of the Kafka event."
    :ser (fn [[key value]] {:key key :value value})
    :des (fn [m] [(:key m) (:value m)])
    :stage 100}
   :byte-array
   {:doc "conversion of byte-array/string."
    :ser (updater (fn [^String s] (.getBytes s "UTF-8")))
    :des (updater (fn [^"[B" bs] (String. bs "UTF-8")))
    :stage 10}
   :edn
   {:doc "conversion of string/edn"
    :ser (updater (fn [data] (pr-str data)))
    :des (updater (fn [s] (edn/read-string s)))
    :stage 20}
   :topic
   {:doc "high order serialization only, attach topic information"
    :ser (fn [topic] (fn [m] (assoc m :topic topic)))
    :stage 0}})

(defn- fn-selector
  "return functions in a `shapers` data structure in order."
  [ks shapers f-pair]
  (->> ks
       (map #(if (vector? %) % [%])) ;normalize 
       (map (fn [[k & args]]
              (let [[order target-f] (some->> (get shapers k) (f-pair))]
                (if (and order target-f)
                  [order (if (seq args) (apply target-f args) target-f)]
                  (throw (ex-info "Invalid shape" {:ks ks})))))) 
       (sort-by first)
       (map second)))

(defn serialize
  "returns a serialization function specified in `ks`"
  ([ks]
   (serialize ks default-shapers))
  ([ks shapers]
   (->> (fn-selector (set ks) shapers (juxt :stage :ser))
        (apply comp))))

(defn deserialize
  "returns a deserialization function specified in `ks`"
  ([ks]
   (deserialize ks default-shapers))
  ([ks shapers]
   (->> (fn-selector (set ks) shapers (juxt :stage :des))
        (reverse)
        (apply comp))))

(comment 
  ((serialize [:byte-array :edn :value-only]) {:string "hello"}) 
  ((deserialize [:byte-array :value-only :edn]) {:value (.getBytes "3")})
  ((serialize [:edn :value-only [:topic "test"]]) {:foo "hello"})
  )
