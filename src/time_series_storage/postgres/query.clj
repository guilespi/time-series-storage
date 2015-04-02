(ns time-series-storage.postgres.query
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb])
  (:use sqlingvo.core
        time-series-storage.postgres.common))

(def sqdb (sqdb/postgresql))

(defn- range-where
  "Retrieves a time-ranged condition for a specific fact in
   a specific dimension path"
  [slice filter-data start finish]
  `(and ~@(for [[k v] filter-data]
                `(= ~k ~v))
        (>= :timestamp ~(get-slice slice start))
        (<= :timestamp ~(get-slice slice finish))))

(defn- best-grouping
  [groupings dimension data]
  (conj
   (first
    ;;always add dimension as part of the grouping keys to enable
    ;;filtering by the last dimension too
    (drop-while #(not= (set (conj (keys data) dimension))
                       (set (conj (keys (select-keys data %)) dimension)))
                groupings))
   dimension))

(defn query
  "Retrieves a particular range of values for the specified fact and dimension."
  [db fact dimension filter-data start finish]
  (let [grouping (best-grouping (:grouped_by dimension)
                                (keyword (:id dimension))
                                filter-data)
        table-name (make-table-name fact grouping)
        slice (or (:slice dimension) (:slice fact))
        condition (range-where slice
                               (->> (select-keys filter-data grouping)
                                    (filter second))
                               start
                               finish)]
    (j/query db
      (sql
       (select sqdb [*]
               (from table-name)
               (where condition))))))
