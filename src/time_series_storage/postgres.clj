(ns time-series-storage.postgres
  (:gen-class)
  (:require [time-series-storage.api :as api :refer [TimeSeries]]
            [time-series-storage.postgres.schema :as schema]
            [time-series-storage.postgres.query :as q]
            [time-series-storage.postgres.update :as u]
            [clj-time.core :as t]
            [clj-time.coerce :as tcoerce])
  (:use [time-series-storage.query]))



(defrecord Postgres [config]
  TimeSeries
  (init-schema! [service]
    (schema/create-facts-table! config)
    (schema/create-dimensions-table! config))

  (drop-schema! [service]
    ;; TODO: run all as a unique transaction
    (schema/drop-facts-time-series-tables! config)
    (schema/drop-facts-table! config)
    (schema/drop-dimensions-table! config))

  (define-fact! [service id type slice options]
    (schema/create-fact! config
                         (keyword id)
                         (keyword type)
                         slice
                         options))

  (define-dimension! [service id options]
    (when-let [grouped-by (:grouped_by options)]
      (doseq [group grouped-by]
        (when-not (schema/get-dimensions config group)
          (throw (Exception. (format "Some specified dimensions to group-by do not exist on:" group))))))

    (schema/create-dimension! config
                              (keyword id)
                              options))

  (facts [service]
    (schema/all-facts config))

  (fact [service fact-id]
    (schema/get-fact config fact-id))

  (dimensions [service]
    (schema/all-dimensions config))

  (new-fact! [service fact-id value categories]
    (api/new-fact! service
                   fact-id
                   (java.util.Date.)
                   value
                   categories))

  (new-fact! [service fact-id timestamp value categories]
    (when (some nil? (vals categories))
      (throw (Exception. "Some categories have nil values")))

    (if-let [fact (api/fact service fact-id)]
      (if-let [dims (->> (schema/get-dimensions config (keys categories))
                         (filter (fn [[k d]] (contains? (:facts d) fact-id)))
                         seq)]
        ;;for each dimension definition update fact in properly grouped tables
        (u/new-fact config
                    fact
                    (tcoerce/from-date timestamp)
                    value
                    categories
                    (into {} dims))
        (throw (Exception. "None of the dimensions specified track the supplied fact")))
      (throw (Exception. (format "Fact %s is not defined" fact-id)))))

  (inc! [service fact-id categories]
    (api/inc! service
              fact-id
              (java.util.Date.)
              categories))

  (inc! [service fact-id timestamp categories]
    (if-let [fact (api/fact service fact-id)]
      (if-let [dims (->> (schema/get-dimensions config (keys categories))
                         (filter (fn [[k d]] (contains? (:facts d) fact-id)))
                         seq)]
        (u/new-fact config
                    fact
                    (tcoerce/from-date timestamp)
                    1
                    categories
                    (into {} dims))
        (throw (Exception. "None of the dimensions specified track the supplied fact")))
      (throw (Exception. (format "Fact %s is not defined" fact-id)))))

  (get-timeseries [service fact dimension query-data start finish step]
    (if-let [fact-def (schema/get-fact config fact)]
      (if-let [dim-def (schema/get-dimension config dimension)]
        (-> (q/query config
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
