(ns build
  (:require
   [clojure.tools.build.api :as b]
   [org.corfield.build :as cb]))

(defn project
  [opts]
  ( merge opts {:lib     'robertluo/waterfall
                :version (format "0.1.%s" (b/git-count-revs nil))
                :scm     {:url "https://github.com/robertluo/waterfall"}}))

(defn tests
  [opts]
  (-> opts (cb/run-task [:dev :test])))

(defn ci
  [opts]
  (-> opts (project) (cb/clean) (tests) (cb/jar)))