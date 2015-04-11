(ns time-series-storage.postgres.schema
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as j]
            [sqlingvo.db :as sqdb]
            [clojure.string :as string])
  (:use sqlingvo.core
        time-series-storage.postgres.common))

(def sqdb (sqdb/postgresql))

(defn get-fact
  "Retrieves a fact definition from database, nil if fact does not exists"
  [db fact]
  (first
   (j/query db
            (sql
             (select sqdb [*]
               (from :facts)
               (where `(= :id ~(name fact))))))))

(defn all-facts
  "Returns all facts defined on the database"
  [db]
  (j/query db
            (sql
             (select sqdb [*]
               (from :facts)))))

(defn all-dimensions
  "Returns all dimensions defined on the database"
  [db]
  (map #(update-in % [:grouped_by] read-string)
       (j/query db
                (sql
                 (select sqdb [*]
                         (from :dimensions))))))


(defn get-dimensions
  "Receives a sequence of dimensions and validates all exist, returning
   a map of definitions keyed by dimension"
  [db s]
  (let [dims (seq (j/query db
                           (sql (select sqdb [*]
                                  (from :dimensions)
                                  (where `(in :id ~(map #(name %) s)))))))
        defs (reduce #(assoc %1 (keyword (:id %2)) (update-in %2 [:grouped_by] read-string))
                     {} dims)]
    ;;if keys do not match some dimension does not exist
    (when (= (set s) (set (keys defs)))
      defs)))

(defn get-dimension
  "Retrieves a specific dimensions given its identifier, supports keyworded
   or named identifier. Returns grouped_by field deserialized."
  [db id]
  (when-let [dim (first
                  (j/query db
                    (sql (select sqdb [*]
                                 (from :dimensions)
                                 (where `(= :id ~(name id)))))))]
    (update-in dim [:grouped_by] read-string)))

(defn create-facts-table!
  [db]
  (j/execute! db
    (sql
     (create-table sqdb :facts
       (if-not-exists true)
       (column :id :varchar :length 40 :primary-key? true)
       (column :name :varchar :length 40)
       (column :type :varchar :length 20 :not-null? true)
       (column :slice :integer :not-null? true :default 15)
       (column :units :varchar :length 20 :not-null? true)
       (column :filler :integer :not-null? true :default 0)
       (column :start :integer)
       (column :end :integer)
       (column :step :integer)))))

(defn drop-facts-table!
  [db]
  (j/execute! db
    (sql
      (drop-table sqdb [:facts]
        (if-exists true)))))

(defn create-dimensions-table!
  [db]
  (j/execute! db
    (sql
     (create-table sqdb :dimensions
       (if-not-exists true)
       (column :id :varchar :length 40 :primary-key? true)
       (column :name :varchar :length 40)
       (column :slice :integer)
       (column :group_only :boolean :default 'FALSE)
       (column :grouped_by :varchar :length 300)))))

(defn drop-dimensions-table!
  [db]
  (j/execute! db
    (sql
      (drop-table sqdb [:dimensions]
        (if-exists true)))))

(defn create-fact!
  "Inserts the new fact to the database"
  [db id type slice {:keys [name filler units
                            start end step]}]
  (j/execute! db
    (sql (insert sqdb :facts []
           (values {:id (clojure.core/name id)
                    :name name
                    :type (clojure.core/name type)
                    :slice (or slice 15)
                    :filler (or filler 0)
                    :units (or units "counter")
                    :start start
                    :end end
                    :step step})))))

(defn make-dimension
  "Returns the query needed to create a dimension table
   for the specified parameters."
  [id {:keys [slice name group_only grouped_by]}]
  (insert sqdb :dimensions []
      (values {:id (clojure.core/name id)
               :name name
               :slice slice
               :group_only (or group_only false)
               :grouped_by (pr-str (or grouped_by [[]]))})))


(defmulti create-fact-column (fn [_ fact] (keyword (:type fact))))

(defmethod create-fact-column :counter
  [stmt fact]
  (second ((column :counter :integer) stmt)))

(defmethod create-fact-column :histogram
  [stmt fact]
  (reduce #(second ((column (str "b" %2) :integer) %1))
          stmt
          (range (:start fact) (:end fact) (:step fact))))

(defmethod create-fact-column :average
  [stmt fact]
  (->> ((column :counter :integer) stmt)
       second
       ((column :total :integer))
       second))

(defn make-time-series-table
  "Dynamically creates a time-series table for a specific fact
   with a set of dimensions, dynamically creates:

     * Columns according to total dimensions
     * Columns according to fact type"
  [fact dims]
  (create-table sqdb (make-table-name fact dims)
    (if-not-exists true)
    ;;primary key is composite on all dimensions + timestamp
    (apply primary-key (concat [:timestamp] dims))
    (column :timestamp :varchar :length 40)
    ;;this looks like magic because it is, a monad sequence transformer
    ;;dynamically appends columns to current statement
    (fn [stmt]
      [nil (reduce #(second ((column %2 :varchar :length 40) %1))
                   stmt
                   dims)])
    (fn [stmt]
      [nil (create-fact-column stmt fact)])
    ))

(defn- drop-time-series-table-stm
  [fact {dim-id :id grouped-by :grouped_by}]
  (->> grouped-by
       (map #(conj % dim-id))
       (map #(make-table-name fact %))
       (map #(drop-table sqdb [%]
                         (if-exists true)))))

(defn- drop-fact-time-series-stmts
  [fact dims]
  (->> (filter (complement :group_only) dims)
       (map #(drop-time-series-table-stm fact %))
       (apply concat)))

(defn drop-facts-time-series-tables!
  [db]
  (let [dims (all-dimensions db)
        tx (->> (all-facts db)
                (map #(drop-fact-time-series-stmts % dims))
                (apply concat))]
    (execute-with-transaction! db (map sql tx))))

(defn create-dimension!
  "In a transaction, creates the dimension register and all the
  tables needed to keep track of all the configured faacts up to
  the moment"
  [db id opts]
  (let [facts (all-facts db)
        grouped-by (or (:grouped_by opts) [[]])
        time-series-tables (when-not (:group_only opts)
                             (for [fact facts
                                   group grouped-by]
                               (make-time-series-table fact (conj group id))))
        tx (conj time-series-tables (make-dimension id opts))]
    (execute-with-transaction! db (map sql tx))))
