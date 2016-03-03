(ns time-series-storage.api)

(defprotocol TimeSeries

  (define-fact! [service id type slice options]
    "Adds a new fact definition to the service database")

  (define-dimension! [service id options]
    "Adds a new dimension defintion to the service database")

  (update-dimension! [service id fact]
    "Updates dimension tracked facts")

  (facts [service]
    "Retrieves all defined facts")

  (fact [service fact-id]
    "Retrieves a fact given its id")

  (dimensions [service]
    "Retrieves all defined dimensions")

  (new-fact! [service id value categories]
             [service id timestamp value categories]
    "Notifies of new event occurred, generates event tracking
     information for the fact on all the specified categories")

  (inc! [service id categories]
        [service id timestamp categories]
    "Notifies of new counter event occurred and increments
     its value in all the specified categories")

  (get-timeseries [service fact dimension query-data start finish]
                  [service fact dimension query-data start finish step]
    "Retrieves the complete timeseries for the queried range of the fact")

  (get-histogram [service fact dimension query-data start finish]
                 [service fact dimension query-data start finish merge-with]
    "Retrieves a single row histogram for the fact, if merge-with
     not specified histogram time-series is merged with +")

  (drop-schema! [service]
    "Drops the schema")

  (init-schema! [service]
    "Initializes schema"))
