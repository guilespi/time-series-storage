(ns time-series-storage.postgres.query
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j])
  (:use sqlingvo.core
        time-series-storage.postgres.common))

(defn- range-where
  "Retrieves a time-ranged condition for a specific fact in
   a specific dimension path"
  [slice filter-data start finish]
  `(and ~@(for [[k v] filter-data]
                `(= ~k ~v))
        (>= :timestamp ~(get-slice slice start))
        (<= :timestamp ~(get-slice slice finish))))

(defn- best-grouping
  [groupings data]
  (first
   (drop-while #(not= (set (keys data))
                      (set (keys (select-keys data %)))) groupings)))

(defn query
  "Retrieves a particular range of values for the specified fact and dimension."
  [db fact dimension filter-data start finish]
  (let [grouping (conj (best-grouping (:grouped_by dimension) filter-data)
                       (keyword (:id dimension)))
        table-name (make-table-name fact grouping)
        slice (or (:slice dimension) (:slice fact))
        condition (range-where slice
                               (->> (select-keys filter-data grouping)
                                    (filter second))
                               start
                               finish)]
    (j/query db
      (sql
       (select [*]
               (from table-name)
               (where condition))))))
