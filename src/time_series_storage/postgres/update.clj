(ns time-series-storage.postgres.update
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb]
            [time-series-storage.postgres.schema :as schema])
  (:use sqlingvo.core
        time-series-storage.postgres.common))

(def sqdb (sqdb/db :postgresql))

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

; From postgres: 63-byte type for storing system identifiers
(def PG_NAME_LENGTH 63)
(def PKEY_SUFFIX "_pkey")

(defn tablename->pkey
  "Returns a valid and unique name for the constraint"
  [table-name]
  (let [hex-hash (format "_%08x" (hash table-name))
        max-length (- PG_NAME_LENGTH (count PKEY_SUFFIX) (count hex-hash))
        prefix (if (> (count table-name) max-length)
                 (subs table-name 0 max-length)
                 table-name)]
    (str prefix hex-hash PKEY_SUFFIX)))

(defmethod make-dimension-fact :counter
  ;;Makes a statement for upserting counters on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name (->> (conj group (:id dimension))
                                  (make-table-name fact))
                  pkey-name (tablename->pkey (name table-name))
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (insert sqdb table-name (conj (keys key) :counter)
                        (values [(vec (conj (vals key) value))])
                        (on-conflict-on-constraint (keyword pkey-name)
                                                   (do-update {:counter (symbol (str "counter+" value))}))))))))

(defmethod make-dimension-fact :average
  ;;Makes a statement for upserting averages on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name (->> (conj group (:id dimension))
                                  (make-table-name fact))
                  pkey-name (tablename->pkey (name table-name))
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (insert sqdb table-name (concat (keys key) [:counter :total])
                        (values [(conj (vec (vals key)) 1 value)])
                        (on-conflict-on-constraint (keyword pkey-name)
                                                   (do-update {:counter '(+ counter 1)
                                                               :total   (symbol (str "total+" value))}))))))))

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
