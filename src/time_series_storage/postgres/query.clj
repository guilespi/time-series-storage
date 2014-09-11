(ns time-series-storage.postgres.query
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j])
  (:use sqlingvo.core
        time-series-storage.postgres.common))

(defn range-where
  "Retrieves a time-ranged condition for a specific fact in
   a specific dimension path"
  [fact dimension filter-data start finish]
  `(and ~@(for [[k v] (merge (select-keys filter-data (:grouped_by dimension))
                             {(keyword (:id dimension)) (get filter-data (keyword (:id dimension)))})]
                `(= ~k ~v))
        (>= :timestamp ~(get-slice (or (:slice dimension)
                                       (:slice fact)) start))
        (<= :timestamp ~(get-slice (or (:slice dimension)
                                       (:slice fact)) finish))))

(defn query
  "Retrieves a particular range of values for the specified fact and dimension."
  [db fact dimension filter-data start finish]
  (let [table-name (->> (conj (:grouped_by dimension) (:id dimension))
                        (make-table-name fact))
        condition (range-where fact dimension filter-data start finish)]
    (j/query db
      (sql
       (select [*]
               (from table-name)
               (where condition))))))
