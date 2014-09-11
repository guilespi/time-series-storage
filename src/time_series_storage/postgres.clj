(ns time-series-storage.postgres
  (:refer-clojure :exclude [distinct group-by])
  (:require [time-series-storage.api :refer [TimeSeries]]
            [clojure.java.jdbc :as j]
            [clj-time.core :as t]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]
            [clojure.string :as string])
  (:use sqlingvo.core))


(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :user "datomic"
              :password "datomic"
              :subname "//localhost:5432/dosierer"})

(defn create-facts-table!
  [db]
  (j/execute! db
    (sql
     (create-table :facts
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

(defn create-dimensions-table!
  [db]
  (j/execute! db
    (sql
     (create-table :dimensions
       (if-not-exists true)
       (column :id :varchar :length 40 :primary-key? true)
       (column :name :varchar :length 40)
       (column :slice :integer)
       (column :group_only :boolean :default 'FALSE)
       (column :grouped_by :varchar :length 300)))))

(defn get-fact
  "Retrieves a fact definition from database, nil if fact does not exists"
  [db fact]
  (first
   (j/query db
            (sql
             (select [*]
               (from :facts)
               (where `(= :id ~(name fact))))))))

(defn all-facts
  [db]
  (j/query db
            (sql
             (select [*]
               (from :facts)))))

(defn all-dimensions
  [db]
  (j/query db
            (sql
             (select [*]
               (from :dimensions)))))

(defn create-fact!
  "Inserts the new fact to the database"
  [db id type slice {:keys [name filler units
                            start end step]}]
  (j/execute! db
    (sql (insert :facts []
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
  (insert :dimensions []
      (values {:id (clojure.core/name id)
               :name name
               :slice slice
               :group_only (or group_only false)
               :grouped_by (pr-str (or grouped_by []))})))

(defn get-dimensions
  "Receives a sequence of dimensions and validates all exist, returning
   a map of definitions keyed by dimension"
  [db s]
  (let [dims (seq (j/query db
                           (sql (select [*]
                                  (from :dimensions)
                                  (where `(in :id ~(map #(name %) s)))))))
        defs (reduce #(assoc %1 (keyword (:id %2)) (update-in %2 [:grouped_by] read-string))
                     {} dims)]
    ;;if keys do not match some dimension does not exist
    (when (= (set s) (set (keys defs)))
      defs)))

(defn get-dimension
  [db id]
  (when-let [dim (first
                  (j/query db
                    (sql (select [*]
                                 (from :dimensions)
                                 (where `(= :id ~(name id)))))))]
    (update-in dim [:grouped_by] read-string)))

(defn make-table-name
  "Given a fact and a sorted list of dimensions retrieves
   the corresponding table name storing the grouped fact"
  [fact dims]
  (-> (mapv name dims)
      (conj (name (:id fact)))
      (#(string/join "_" %))
      keyword))

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
  (create-table (make-table-name fact dims)
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

(defn create-dimension!
  "In a transaction, creates the dimension register and all the
   tables needed to keep track of all the configured faacts up to
   the moment"
  [db id opts]
  (if (or (nil? (seq (:grouped_by opts)))
          (get-dimensions db (:grouped_by opts)))
    (let [facts (all-facts db)
          dimension (conj (:grouped_by opts) id)
          tx (concat
              [(make-dimension id opts)]
              (when (not (:group_only opts))
                (for [f facts]
                  (make-time-series-table f dimension))))]
      (j/with-db-transaction [t db]
        (doseq [st tx]
          (j/execute! t
                      (sql st)))))
    (throw (Exception. "Some specified dimensions to group-by do not exist"))))


(defn get-slice
  ([slice-size]
     (get-current-slice slice-size (t/now)))
  ([slice-size date]
     (let [minutes (* (int (/ (t/minute date) slice-size)) slice-size)]
       (tformat/unparse (tformat/formatters :basic-date-time)
                        (t/date-time (t/year date)
                                     (t/month date)
                                     (t/day date)
                                     (t/hour date)
                                     minutes)))))

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
  [keyvals]
  `(and ~@(for [[k v] keyvals]
            `(= ~k ~v))))

(defmulti make-dimension-fact (fn [f _ _] (keyword (:type f))))

(defmethod make-dimension-fact :counter
  [fact dimension event]
  (let [table-name (->> (conj (:grouped_by dimension) (:id dimension))
                        (make-table-name fact))
        key (event-key fact dimension event)]
    (with [:upsert (update table-name '((= counter counter+1))
                         (where (expand-condition key))
                         (returning *))]
        (insert table-name (conj (keys key) :counter)
                (select (conj (vec (vals key)) 1))
                (where `(not-exists ~(select [*] (from :upsert))))))))

(defmethod make-dimension-fact :average
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
  (if-let [fact (get-fact db id)]
    ;;TODO:this should be cached on key set!
    (if-let [dims (get-dimensions db (keys categories))]
      ;;for each dimension definition update fact in properly grouped tables
      (let [tx (for [d (filter #(not (:group_only %)) (vals dims))]
                 (make-dimension-fact fact d (merge categories {id value})))]
        (j/with-db-transaction [t db]
          (doseq [st tx]
            (j/execute! t
                        (sql st)))))
      (throw (Exception. "Some specified dimensions do not exist")))
    (throw (Exception. (format "Fact %s is not defined" id)))))


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
  [db fact dimension filter-data start finish]
  (let [table-name (->> (conj (:grouped_by dimension) (:id dimension))
                        (make-table-name fact))
        condition (range-where fact dimension filter-data start finish)]
    (j/query db
      (sql
       (select [*]
               (from table-name)
               (where condition))))))

(defrecord Postgres [config]
  TimeSeries
  (add-fact [service id type slice options]
    )

  (add-dimension [service id options]
    )

  (new-fact [service id value categories]
    )

  (get-timeseries [service fact dimensions start finish]
    )

  (get-histogram [service fact dimensions start finish]
                 [service fact dimensions start finish merge-with]
    ))

(comment

  ;;create-counter
  (create-fact! db-spec :registros :counter 15 {:name "Cantidad de registros"
                                                :filler 0
                                                :units "counter"})

  ;;create-average
  (create-fact! db-spec :avg_time :average 15 {:name "Tiempo promedio"
                                               :filler 0
                                               :units "seconds"})
  ;;create-histogram
  (create-fact! db-spec :time-distr :histogram 15 {:name "Histograma de tiempo"
                                                   :filler 0
                                                   :units "seconds"
                                                   :start 0
                                                   :end 1000
                                                   :step 100})

  ;;
  (create-dimension! db-spec :company {:group_only true :name "Compania"})
  (create-dimension! db-spec :campaign {:grouped_by [:company] :name "Campania"})
  (create-dimension! db-spec :channel {:grouped_by [:company :campaign] :name "Canal"})

  (create-dimension! db-spec :dependency {:name "Dependencia de Correo"})
  (create-dimension! db-spec :dependency_user {:grouped_by [:dependency] :name "Usuario"})

  (new-fact db-spec :registros 1 {:dependency "32" :dependency_user "juanele"})
  (new-fact db-spec :avg_time 15 {:company "bbva" :campaign "ventas" :channel "web"})
  (new-fact db-spec :avg_time 15 {:company "bbva" :campaign "ventas" :channel "mail"})

  (query db-spec
         (get-fact db-spec :avg_time)
         (get-dimension db-spec :campaign)
         {:company "bbva" :campaign "ventas"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (query db-spec
         (get-fact db-spec :avg_time)
         (get-dimension db-spec :channel)
         {:company "bbva" :campaign "ventas" :channel "web"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (query db-spec
         (get-fact db-spec :registros)
         (get-dimension db-spec :dependency)
         {:dependency "32"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (query db-spec
         (get-fact db-spec :registros)
         (get-dimension db-spec :dependency)
         {:dependency "35"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  )
