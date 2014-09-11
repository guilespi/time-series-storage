(ns time-series-storage.postgres.update
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j]
            [time-series-storage.postgres.schema :as schema])
  (:use sqlingvo.core
        time-series-storage.postgres.common))


(defn event-key
  "Returns the particular key for updating a fact in a specific dimension.
  This WILL consider timestamp as one of the default key columns, and WILL calculate
  the bucket according to time."
  [fact dimension event]
  (merge (select-keys event (:grouped_by dimension))
         {(keyword (:id dimension)) (get event (keyword (:id dimension)))
          :timestamp (get-slice (or (:slice dimension)
                                    (:slice fact)))}))

(defn expand-condition
  "Given a map of key-values creates a condition assuming equality
   for each pair, meaning:

      {:a 1 :b 2} => (and (= a 1) (= b 2))

   Condition is usable in sqlingvo where clauses"
  [keyvals]
  `(and ~@(for [[k v] keyvals]
            `(= ~k ~v))))

(defmulti make-dimension-fact (fn [f _ _] (keyword (:type f))))

(defmethod make-dimension-fact :counter
  ;;Makes a statement for upserting counters on a specific fact and
  ;;dimension hierarchy
  [fact dimension event]
  (let [table-name (->> (conj (:grouped_by dimension) (:id dimension))
                        (make-table-name fact))
        key (event-key fact dimension event)]
    (with [:upsert (update table-name '((= counter counter+1))
                         (where (expand-condition key))
                         (returning *))]
        (insert table-name (conj (keys key) :counter)
                (select (conj (vals key) 1))
                (where `(not-exists ~(select [*] (from :upsert))))))))

(defmethod make-dimension-fact :average
  ;;Makes a statement for upserting averages on a specific fact and
  ;;dimension hierarchy
  [fact dimension event]
  (let [table-name (->> (conj (:grouped_by dimension) (:id dimension))
                        (make-table-name fact))
        key (event-key fact dimension event)
        value (get event (keyword (:id fact)))]
    (with [:upsert (update table-name (conj '()
                                            '(= counter counter+1)
                                            (concat '(= total)
                                                    [(symbol (str "total+" value))]))
                         (where (expand-condition key))
                         (returning *))]
          (insert table-name (concat (keys key) [:counter :total])
                  (select (conj (vec (vals key)) 1 value))
                  (where `(not-exists ~(select [*] (from :upsert))))))))

(defn new-fact
  "When a new fact occurs update all the corresponding dimensions specified in the fact
   categories. If some category is not updatable the complete fact fails (this is in
   order to avoid counter mismatches)"
  [db id value categories]
  (if-let [fact (schema/get-fact db id)]
    ;;TODO:this should be cached on key set!
    (if-let [dims (schema/get-dimensions db (keys categories))]
      ;;for each dimension definition update fact in properly grouped tables
      (let [tx (for [d (filter #(not (:group_only %)) (vals dims))]
                 (make-dimension-fact fact d (merge categories {id value})))]
        (j/with-db-transaction [t db]
          (doseq [st tx]
            (j/execute! t
                        (sql st)))))
      (throw (Exception. "Some specified dimensions do not exist")))
    (throw (Exception. (format "Fact %s is not defined" id)))))
