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

  (add-fact! [service id type slice options]
    (schema/create-fact! config
                         (keyword id)
                         (keyword type)
                         slice
                         options))

  (add-dimension! [service id options]
    (schema/create-dimension! config
                              (keyword id)
                              options))

  (facts [service]
    (schema/all-facts config))

  (dimensions [service]
    (schema/all-dimensions config))

  (new-fact! [service id value categories]
    (api/new-fact! config
               (t/now)
               id
               value
               categories))

  (new-fact! [service timestamp id value categories]
    (u/new-fact config
                (tcoerce/from-date timestamp)
                id
                value
                categories))

  (inc! [service id categories]
    (api/inc! service (t/now) id categories))

  (inc! [service timestamp id categories]
    (u/new-fact config
                (tcoerce/from-date timestamp)
                id
                1
                categories))

  (get-timeseries [service fact dimension query-data start finish step]
    (if-let [fact-def (schema/get-fact config fact)]
      (if-let [dim-def (schema/get-dimension config dimension)]
        (let [data-points (q/query config
                                   fact-def
                                   dim-def
                                   query-data
                                   (tcoerce/from-date start)
                                   (tcoerce/from-date finish))]
          (if step
            (fill-range start
                        finish
                        step
                        (collapse data-points step))
            data-points))
        (throw (Exception. (format "Non existent dimension %s specified. Please check your schema" dimension))))
      (throw (Exception. (format "Non existent fact %s specified. Please check your schema." fact)))))

  (get-timeseries [service fact dimension query-data start finish]
    (api/get-timeseries service fact dimension query-data start finish nil))

  (get-histogram [service fact dimension query-data start finish]
                 [service fact dimension query-data start finish merge-with]
    ))
