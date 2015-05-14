(ns time-series-storage.query
  (:require [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]
            [clj-time.core :as t]))

(defn time-dimension
  [collapse-by row]
  (let [date (tcoerce/from-string (:timestamp row))
        dow (t/day-of-week date)]
    (condp = collapse-by
      :day (tformat/unparse (tformat/formatters :basic-date-time)
                            (t/date-time (t/year date)
                                         (t/month date)
                                         (t/day date)
                                         0
                                         0))
      :hour (tformat/unparse (tformat/formatters :basic-date-time)
                             (t/date-time (t/year date)
                                          (t/month date)
                                          (t/day date)
                                          (t/hour date)
                                          0))
      :week (let [start-of-week (t/minus- date (t/days (t/day-of-week date)))]
              (tformat/unparse (tformat/formatters :basic-date-time)
                               (t/date-time (t/year start-of-week)
                                            (t/month start-of-week)
                                            (t/day start-of-week)
                                            0
                                            0)))
      :month (tformat/unparse (tformat/formatters :basic-date-time)
                              (t/date-time (t/year date)
                                           (t/month date)
                                           1
                                           0
                                           0))
      :none :all)))

;;TODO: this should be done inside the library with knowledge about
;;the dimension type not here infering stuff
(defmulti collapse (fn [[r & rest] by]
                     (cond
                      (and (:counter r)
                           (:total r)) :average
                      (:counter r) :counter
                      :else :default)))

(defmethod collapse :counter
  [rows by]
  (apply merge
   (for [[k v] (group-by #(select-keys % (keys (dissoc % :counter :timestamp)))
                         rows)]
     {k  (->> (group-by (partial time-dimension by) v)
              (map (fn [[k v]]
                     {(or (tcoerce/from-string k)
                          k) (reduce #(+ %1 (:counter %2)) 0 v)}))
              (apply merge))})))


(defmethod collapse :average
  [rows by]

  (apply merge
         (for [[k v] (group-by #(select-keys % (keys (dissoc % :counter :timestamp :total)))
                               rows)]
           {k (->> (group-by (partial time-dimension by) v)
                (map (fn [[k v]]
                       {(or (tcoerce/from-string k) k) (reduce #(merge-with + %1 (select-keys %2 [:counter
                                                                                           :total]))
                                                        {:counter 0
                                                         :total 0}
                                                        v)}))
                (apply merge))})))

(defmethod collapse :histogram
  [rows by]
  (throw (Exception. "histogram collapsing not yet ready")))


(defmethod collapse :default
  [rows by]
  {})

(defn time-range
  [start finish step]
  (when (t/before? start finish)
    (lazy-seq
     (cons start
           (time-range
            (t/plus- start (condp = step
                             :day (t/days 1)
                             :week (t/weeks 1)
                             :month (t/months 1)
                             :year (t/years 1)
                             :hour (t/hours 1)))
            finish
            step)))))

(defn fill-range
  [data start finish step]
  (assert step "step must not be nil. consider passing :none")
  (if (= :none step)
    data
    (apply merge
           (for [[k series] data]
             {k (apply merge
                       (for [date (time-range (tcoerce/from-date start)
                                              (tcoerce/from-date finish)
                                              step)]
                         ;;TODO the filler should be by dimension definition
                         {(tcoerce/to-date date) (or (get series date) 0)}))}))))

(defn collapse-and-fill-range [data start finish step]
  (-> (collapse data step)
      (fill-range start
                  finish
                  step)))
