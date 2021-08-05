(ns time-series-storage.sql-server.update
  (:refer-clojure :exclude [distinct group-by update])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb]
            [clojure.string :as string]
            [time-series-storage.postgres.schema :as schema])
  (:use sqlingvo.core
        time-series-storage.sql-server.common))

(def sqdb (sqdb/sqlserver))

(defn- format-name
  [n]
  (-> n
      name
      (string/replace "-" "_")
      (string/replace ":" "")))

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

(defn expand-matcher
  [keyvals params]
  (->> (for [[k v] keyvals]
         (do (swap! params #(conj % v))
             (format "[%s] = ?" (format-name k))))
       (string/join " AND ")))

(def upsert-query-string
  "BEGIN
    UPDATE %s SET %s WHERE %s
    IF @@ROWCOUNT = 0
      INSERT INTO %s (%s) VALUES(%s)
  END")

(defmulti make-dimension-fact (fn [f _ _ _] (keyword (:type f))))

(defmethod make-dimension-fact :counter
  ;;Makes a statement for upserting counters on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name (format-name (->> (conj group (:id dimension))
                                               (make-table-name fact)))
                  value (get event (:id fact))
                  params (atom [value])]
              (when-let [key (event-key fact dimension group event date-time)]
                (vec (concat
                       [(format upsert-query-string
                                (name table-name)
                                (str "[counter] = [counter] + ?")
                                (expand-matcher key params)
                                (name table-name)
                                (string/join ", " (map #(str "[" (format-name %) "]") (cons :counter (keys key))))
                                (string/join ", " (map (constantly "?") (cons :counter (vals key)))))]
                       @params
                       [value]
                       (vals key))))))))

(defmethod make-dimension-fact :average
  ;;Makes a statement for upserting averages on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name  (format-name (->> (conj group (:id dimension))
                                                (make-table-name fact)))
                  value (get event (:id fact))
                  params (atom [value])]
              (when-let [key (event-key fact dimension group event date-time)]
                (vec (concat
                       [(format upsert-query-string
                                (name table-name)
                                "[counter] = [counter] + 1, [total] = [total] + ?"
                                (expand-matcher key params)
                                (name table-name)
                                (string/join ", " (map #(str "[" (format-name %) "]") (concat (keys key) [:counter :total])))
                                (string/join ", " (map (constantly "?") (concat (vals key) [:counter :total]))))]
                       @params
                       (vals key)
                       [1 value])))))))

(defn new-fact
  "When a new fact occurs update all the corresponding dimensions specified in the fact
   categories. If some category is not updatable the complete fact fails (this is in
   order to avoid counter mismatches)"
  [db {fact-id :id :as fact} timestamp value categories dims]
  (let [event (merge categories {fact-id value})
        tx (->> (vals dims)
                (filter #(not (:group_only %)))
                ;; make-dimension-fact returns a list of prepared statements
                ;; i.e.: a vector of a sql string and parameters)
                ;; we concat all of them in order to make a single list
                (mapcat #(make-dimension-fact fact % event timestamp)))]
    (execute-with-transaction! db tx)))
