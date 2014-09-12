(ns time-series-storage.postgres
  (:require [time-series-storage.api :refer [TimeSeries]]
            [time-series-storage.postgres.schema :as schema]
            [time-series-storage.postgres.query :as q]
            [time-series-storage.postgres.update :as u]
            [clj-time.coerce :as tcoerce]))


(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :user "datomic"
              :password "datomic"
              :subname "//localhost:5432/dosierer"})

(defrecord Postgres [config]
  TimeSeries
  (add-fact [service id type slice options]
    (schema/create-fact! db-spec
                         (keyword id)
                         (keyword type)
                         slice
                         options))

  (add-dimension [service id options]
    (schema/create-dimension! db-spec
                              (keyword id)
                              options))

  (new-fact [service id value categories]
    (u/new-fact db-spec
                id
                value
                categories))

  (get-timeseries [service fact dimension query-data start finish]
    (if-let [fact-def (schema/get-fact db-spec fact)]
      (if-let [dim-def (schema/get-dimension db-spec dimension)]
        (q/query db-spec
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
