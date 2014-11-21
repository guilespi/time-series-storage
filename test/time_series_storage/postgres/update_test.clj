(ns time-series-storage.postgres.update-test
  (:require [sqlingvo.core :as sql]
            [clj-time.coerce :as tcoerce] 
            [time-series-storage.postgres.update :as update])
  (:use clojure.test))


(deftest make-dimension-fact-counter
  
  (let [fact {:type :counter
              :id :signup}
        dim {:id "dependency" :slice 15 :grouped_by [[]]}
        event {:dependency 7
               :signup 3}
        date-time (tcoerce/from-string "2014-07-14")
        stmt (update/make-dimension-fact fact
                                         dim
                                         event
                                         date-time)
        sql-str (apply str (apply sql/sql stmt))]
    (is (re-find #"counter = counter\+3" sql-str)))
  )

