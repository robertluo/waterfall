(ns user
  (:require [nextjournal.clerk :as clerk]))

(clerk/serve! {:watch-paths ["notebook"]})

(comment
  (clerk/show! "notebook/introduction.clj")
  )