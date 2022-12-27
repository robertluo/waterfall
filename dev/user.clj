(ns user)

(comment
  (require '[nextjournal.clerk :as clerk])
  (clerk/serve! {:watch-paths ["notebook"]})
  (clerk/show! "notebook/introduction.clj")
  )