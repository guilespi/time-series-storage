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
  [keyvals]
  (->> (for [[k v] keyvals]
         (format "[%s] = '%s'" (format-name k) v))
       (string/join " AND ")))

(def upsert-query-string ""
  "IF EXISTS (select * FROM %s WITH (updlock,serializable) where %s)
     BEGIN
          UPDATE %s WITH (updlock)
          SET %s
          WHERE %s
     END
   ELSE
     BEGIN
          INSERT %s
          (%s) VALUES (%s)
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
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (format upsert-query-string
                        (name table-name)
                        (expand-matcher key)
                        (name table-name)
                        (str "[counter] = [counter] + " value)
                        (expand-matcher key)
                        (name table-name)
                        (string/join ", " (map #(str "["(format-name %)"]") (conj (keys key) :counter)))
                        (string/join ", " (conj (map #(str "'" % "'") (vals key)) value))))))))

(defmethod make-dimension-fact :average
  ;;Makes a statement for upserting averages on a specific fact and
  ;;dimension hierarchy
  [fact dimension event date-time]
  ;;some groupings may not generate upserts when data is missing
  (filter identity
          (for [group (:grouped_by dimension)]
            (let [table-name  (format-name (->> (conj group (:id dimension))
                                                (make-table-name fact)))
                  value (get event (:id fact))]
              (when-let [key (event-key fact dimension group event date-time)]
                (format upsert-query-string
                        (name table-name)
                        (expand-matcher key)
                        (name table-name)
                        (format "[counter] = [counter] + 1, [total] = [total] + %s" value)
                        (expand-matcher key)
                        (name table-name)
                        (string/join ", " (map #(str "["(format-name %)"]") (concat (keys key) [:counter :total])))
                        (string/join ", " (concat (map #(str "'" % "'") (vals key)) [1 value]))))))))

(defn new-fact
  "When a new fact occurs update all the corresponding dimensions specified in the fact
   categories. If some category is not updatable the complete fact fails (this is in
   order to avoid counter mismatches)"
  [db {fact-id :id :as fact} timestamp value categories dims]
  (let [event (merge categories {fact-id value})
        tx (->> (vals dims)
                (filter #(not (:group_only %)))
                (map #(make-dimension-fact fact % event timestamp))
                ;;according to the docs for clojure.java.jdbc/execute! it takes a collection of sql
                ;;(prepared statement or string). In the function execute-with-transaction exeute is called in a doseq.
                ;;When there is more than one element in the collections execute! assumes that the first element of
                ;;the collection is the statement, and the rest of the collections are parameters.
                ;; That is the short story of why I made this join.
                (map #(conj [] (string/join "\n" %))))]
    (execute-with-transaction! db tx)))
