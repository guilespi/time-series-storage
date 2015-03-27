(ns time-series-storage.postgres.update
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j]
            [time-series-storage.postgres.schema :as schema])
  (:use sqlingvo.core
        time-series-storage.postgres.common))


(defn event-key
  "Returns the particular key for updating a fact in a specific dimension.
  This WILL consider timestamp as one of the default key columns, and WILL calculate
  the bucket according to time.

  If some required value for the key is not present in the event, nil is returned."
  [fact dimension group event datetime]
  (when (= (set group)
           (-> (select-keys event group)
               keys
               set))
      (merge (select-keys event group)
             {(keyword (:id dimension)) (get event (keyword (:id dimension)))
              :timestamp (get-slice (or (:slice dimension)
                                        (:slice fact))
                                    datetime)})))

(defn expand-condition
  "Given a map of key-values creates a condition assuming equality
   for each pair, meaning:

      {:a 1 :b 2} => (and (= a 1) (= b 2))

   Condition is usable in sqlingvo where clauses"
  [keyvals]
  `(and ~@(for [[k v] keyvals]
            `(= ~k ~v))))

(defmulti make-dimension-fact (fn [f _ _ _] (keyword (:type f))))

(defmethod make-dimension-fact :counter
  ;;Makes a statement for upserting counters on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name (->> (conj group (:id dimension))
                                  (make-table-name fact))
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (with [:upsert (update table-name `((:= ~'counter
                                                        ~(symbol (str "counter+" value))))
                                       (where (expand-condition key))
                                       (returning *))]
                      (insert table-name (conj (keys key) :counter)
                              (select (conj (vals key) value))
                              (where `(not-exists ~(select [*] (from :upsert)))))))))))

(defmethod make-dimension-fact :average
  ;;Makes a statement for upserting averages on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name (->> (conj group (:id dimension))
                                  (make-table-name fact))
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (with [:upsert (update table-name (conj '()
                                                        '(= counter counter+1)
                                                        (concat '(= total)
                                                                [(symbol (str "total+" value))]))
                                       (where (expand-condition key))
                                       (returning *))]
                      (insert table-name (concat (keys key) [:counter :total])
                              (select (conj (vec (vals key)) 1 value))
                              (where `(not-exists ~(select [*] (from :upsert)))))))))))

(defn new-fact
  "When a new fact occurs update all the corresponding dimensions specified in the fact
   categories. If some category is not updatable the complete fact fails (this is in
   order to avoid counter mismatches)"
  [db {fact-id :id :as fact} timestamp value categories dims]
  (let [event (merge categories {fact-id value})
        tx (->> (vals dims)
                (filter #(not (:group_only %)))
                (map #(make-dimension-fact fact % event timestamp))
                (apply concat))]
    (execute-with-transaction! db (map sql tx))))
