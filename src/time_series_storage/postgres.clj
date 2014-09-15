(ns time-series-storage.postgres
  (:require [time-series-storage.api :refer [TimeSeries]]
            [time-series-storage.postgres.schema :as schema]
            [time-series-storage.postgres.query :as q]
            [time-series-storage.postgres.update :as u]
            [clj-time.coerce :as tcoerce]))


(defrecord Postgres [config]
  TimeSeries
  (init-schema [service]
    (schema/create-facts-table! config)
    (schema/create-dimensions-table! config))

  (add-fact [service id type slice options]
    (schema/create-fact! config
                         (keyword id)
                         (keyword type)
                         slice
                         options))

  (add-dimension [service id options]
    (schema/create-dimension! config
                              (keyword id)
                              options))

  (new-fact [service id value categories]
    (u/new-fact config
                id
                value
                categories))

  (get-timeseries [service fact dimension query-data start finish]
    (if-let [fact-def (schema/get-fact config fact)]
      (if-let [dim-def (schema/get-dimension config dimension)]
        (q/query config
                 fact-def
                 dim-def
                 query-data
                 (tcoerce/from-date start)
                 (tcoerce/from-date finish))
        (throw (Exception. (format "Non existent dimension %s specified. Please check your schema" dimension))))
      (throw (Exception. (format "Non existent fact %s specified. Please check your schema." fact)))))

  (get-histogram [service fact dimension query-data start finish]
                 [service fact dimension query-data start finish merge-with]
    ))
