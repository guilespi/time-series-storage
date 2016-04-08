(ns time-series-storage.sql-server.common
  (:require [clj-time.core :as t]
            [clj-time.format :as tformat]
            [clojure.java.jdbc :as j]
            [clojure.string :as string]))

(defn get-slice
  "Gets the corresponding slice for the specified date-time given the slice-size.

   Will always round to the slice before, so if time is 16:46 and size is 15 slice
   is going to be 16:45.

   Returns a properly date-sortable string"
  ([slice-size]
     (get-slice slice-size (t/now)))
  ([slice-size date]
     (let [minutes (* (int (/ (t/minute date) slice-size)) slice-size)]
       (tformat/unparse (tformat/formatters :basic-date-time)
                        (t/date-time (t/year date)
                                     (t/month date)
                                     (t/day date)
                                     (t/hour date)
                                     minutes)))))

(defn make-table-name
  "Given a fact and a sorted list of dimensions retrieves
   the corresponding table name storing the grouped fact"
  [fact dims]
  (-> (mapv name dims)
      (conj (name (:id fact)))
      (#(string/join "_" %))
      keyword))

(defn execute-with-transaction!
  [db tx]
  (j/with-db-transaction [t db]
    (doseq [st tx]
      (j/execute! t st))))
