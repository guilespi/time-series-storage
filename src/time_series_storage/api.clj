(ns time-series-storage.api)

(defprotocol TimeSeries

  (add-fact! [service id type slice options]
    "Adds a new fact to the service database")

  (add-dimension! [service id options]
    "Adds a new dimension to the service database")

  (facts [service]
    "Retrieves all defined facts")

  (dimensions [service]
    "Retrieves all defined dimensions")

  (new-fact! [service id value categories]
             [service timestamp id value categories]
    "Notifies of new event occurred, generates event tracking
     information for the fact on all the specified categories")

  (inc! [service id categories]
        [service timestamp id categories]
    "Notifies of new counter event occurred and increments
     its value in all the specified categories")

  (get-timeseries [service fact dimension query-data start finish]
                  [service fact dimension query-data start finish step]
    "Retrieves the complete timeseries for the queried range of the fact")

  (get-histogram [service fact dimension query-data start finish]
                 [service fact dimension query-data start finish merge-with]
    "Retrieves a single row histogram for the fact, if merge-with
     not specified histogram time-series is merged with +")

  (init-schema! [service]
    "Initializes schema"))
