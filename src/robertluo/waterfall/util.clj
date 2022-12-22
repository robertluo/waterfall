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

(defmacro optional-require
  "optionally try requre `require-clause`, if success, run `if-body`,
   else `else-body`"
  [require-clause if-body else-body]
  (if
   (try
     (require require-clause)
     true
     (catch Exception _ 
       false))
    if-body
    else-body))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defmacro import-fns
  "import functions from `var-syms`.
   Referred from potemkin library.
   See https://github.com/clj-commons/potemkin/blob/master/src/potemkin/namespaces.clj"
  [var-syms]
  `(do
     ~@(mapcat (fn [sym]
                 (let [vr (resolve sym)
                       m (meta vr)
                       n (:name m)]
                   `[(def ~n ~(deref vr))
                     (alter-meta! (var ~n) merge (dissoc (meta ~vr) :name))]))
            var-syms)))

(comment
  (ns-resolve *ns* 'scala-vo->map)
  (->config-map {:bootstrap-servers "localhost:9092" :group-id "test"})
  (macroexpand-1 '(scala-vo->map my Duration [k v])) 
  (macroexpand-1 '(optional-require clojure.core (a defn [] "ok")))
  (macroexpand-1 '(import-fns [clojure.core/+]))
  )