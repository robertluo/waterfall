(ns robertluo.waterfall.util
  "Utilities" 
  (:require [clojure.string :as str]))

(defn ->config-map
  "turns map `m` into a kafka config map"
  [m]
  (let [kw->str (fn [kw] (str/replace (name kw) "-" "."))]
    (into {} (map (fn [[k v]] [(kw->str k) v])) m)))

(defmacro scala-vo->map
  "define a function `fn-name`, it will turns scala value class
   `clazz` to a map, with `method-names` as its keys."
  [fn-name clazz method-names]
  (let [keys (mapv #(-> % str keyword) method-names)
        obj (gensym "obj")
        values (mapv #(list '. obj (->> % name symbol)) method-names)]
    `(defn ~fn-name
       [^{:tag clazz} ~obj]
       (zipmap ~keys ~values))))

(comment
  (->config-map {:bootstrap-servers "localhost:9092" :group-id "test"})
  (macroexpand-1 '(scala-vo->map my Duration [k v]))
  )