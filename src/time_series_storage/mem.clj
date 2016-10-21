(ns time-series-storage.mem
  (:gen-class)
  (:require [time-series-storage.api :as api :refer [TimeSeries]]
            [clj-time.core :as t]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.string :as string])
  (:use [time-series-storage.query]))

(defn get-slice
  ([slice-size]
     (get-slice slice-size (t/now)))
  ([slice-size date]
     (let [minutes (* (int (/ (t/minute date) slice-size)) slice-size)]
       (tformat/unparse (tformat/formatters :basic-date-time)
                        (t/date-time (t/year date)
                                     (t/month date)
                                     (t/day date)
                                     (t/hour date)
                                     minutes)))))
(defn event-key
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
(defn make-storage-name
  [fact dims]
  (-> (mapv name dims)
      (conj (name (:id fact)))
      (#(string/join "_" %))
      keyword))

(defmulti update-dimension-fact! (fn [service f _ _ _]
                                (keyword (:type f))))

(defmethod update-dimension-fact! :counter
  [service fact dimension event date-time]
  (doseq [group (:grouped_by dimension)]
    (let [storage-name (->> (conj group (:id dimension))
                            (make-storage-name fact))
          value (get event (:id fact))]
      (when-let [key (event-key fact dimension group event date-time)]
        (swap! service
               (fn [config]
                 (update-in config [:data storage-name key :counter] #((fnil + 0) % value))))))))

(defmethod update-dimension-fact! :average
  [service fact dimension event date-time])

(defn new-fact
  [service {fact-id :id :as fact} timestamp value categories dims]
  (let [event (merge categories {fact-id value})
        tx (->> (vals dims)
                (filter #(not (:group_only %))))]
    (doseq [t tx]
      (update-dimension-fact! service fact t event timestamp))))

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
        storage-name (make-storage-name fact grouping)
        slice (or (:slice dimension) (:slice fact))
        filter-data (->> (select-keys filter-data grouping)
                         (filter second)
                         (into {}))
        start-slice (get-slice slice start)
        end-slice (get-slice slice finish)]
    (->> (get (:data @db) storage-name)
         (filter (fn [[entry value]]
                   (and (= (select-keys entry (keys filter-data))
                           filter-data)
                        (<= (compare (:timestamp entry) end-slice) 0)
                        (>= (compare (:timestamp entry) start-slice) 0))))
         (map (fn [[entry value]]
                (merge entry value))))))


(defrecord Mem [config]
  TimeSeries
  (init-schema! [service]
    (swap! config assoc :facts {})
    (swap! config assoc :dimensions {})
    (swap! config assoc :data {}))

  (drop-schema! [service]
    (api/init-schema! service))

  (define-fact! [service id type slice options]
    (swap! config assoc-in
           [:facts (keyword id)]
           (merge options
                  {:type (name type)
                   :slice slice
                   :id (name id)})))

  (define-dimension! [service id options]
    (when-let [grouped-by (:grouped_by options)]
      (doseq [group grouped-by]
        (when (not= (count (select-keys (:dimensions @config) group))
                    (count group))
          (throw (Exception. (format "Some specified dimensions to group-by do not exist on:" group))))))
    (swap! config assoc-in
           [:dimensions (keyword id)]
           (merge options
                  {:id (name id)
                   :grouped_by (or (:grouped_by options) [[]])
                   :group_only (or (:group_only options) false)
                   :facts (if (:facts options) (set (:facts options)) #{})})))

  (update-dimension! [service id fact-id]
    (if-let [fact (api/fact service fact-id)]
      (throw (Exception. "not implemented"))
      (throw (Exception. (format "Fact %s is not defined" fact-id)))))


  (facts [service]
    (map second (:facts @config)))

  (fact [service fact-id]
    (get (:facts @config) fact-id))

  (dimensions [service]
    (map second (:dimensions @config)))

  (new-fact! [service fact-id value categories]
    (api/new-fact! config
                   fact-id
                   (java.util.Date.)
                   value
                   categories))

  (new-fact! [service fact-id timestamp value categories]
    (when (some nil? (vals categories))
      (throw (Exception. "Some categories have nil values")))

    (if-let [fact (api/fact service fact-id)]
      (if-let [dims (->> (select-keys (:dimensions @config) (keys categories))
                         (filter (fn [[k d]] (contains? (:facts d) fact-id)))
                         seq)]
        ;;for each dimension definition update fact in properly grouped tables
        (new-fact config
                  fact
                  (tcoerce/from-date timestamp)
                  value
                  categories
                  (into {} dims))
        (throw (Exception. "None of the dimensions specified track the supplied fact")))
      (throw (Exception. (format "Fact %s is not defined" fact-id)))))

  (inc! [service fact-id categories]
    (api/inc! config
              fact-id
              (java.util.Date.)
              categories))

  (inc! [service fact-id timestamp categories]
    (if-let [fact (api/fact service fact-id)]
      (if-let [dims (->> (select-keys (:dimensions @config) (keys categories))
                         (filter (fn [[k d]] (contains? (:facts d) fact-id)))
                         seq)]
        (new-fact config
                  fact
                  (tcoerce/from-date timestamp)
                  1
                  categories
                  (into {} dims))
        (throw (Exception. "None of the dimensions specified track the supplied fact")))
      (throw (Exception. (format "Fact %s is not defined" fact-id)))))

  (get-timeseries [service fact dimension query-data start finish step]
    (if-let [fact-def (get (:facts @config) fact)]
      (if-let [dim-def (get (:dimensions @config) dimension)]
        (-> (query config
                   fact-def
                   dim-def
                   query-data
                   (tcoerce/from-date start)
                   (tcoerce/from-date finish))
            (collapse-and-fill-range
             start
             finish
             (or step :none)))
        (throw (Exception. (format "Non existent dimension %s specified. Please check your schema" dimension))))
      (throw (Exception. (format "Non existent fact %s specified. Please check your schema." fact)))))

  (get-timeseries [service fact dimension query-data start finish]
    (api/get-timeseries service fact dimension query-data start finish nil))

  (get-histogram [service fact dimension query-data start finish]
                 [service fact dimension query-data start finish merge-with]
    ))


(defn make
  []
  (->Mem (atom {})))
