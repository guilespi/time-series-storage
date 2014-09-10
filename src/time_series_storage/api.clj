(ns time-series-storage.api)

(defprotocol TimeSeries

  (add-fact [service id type slice options]
    "Adds a new fact to the service database")

  (add-dimension [service id options]
    "Adds a new dimension to the service database")

  (new-fact [service id value categories]
    "Notifies of new event occurred, generates event tracking
     information for the fact on all the specified categories")

  (get-timeseries [service fact dimensions start finish]
    "Retrieves the complete timeseries for the queried range of the fact")

  (get-histogram [service fact dimensions start finish]
                 [service fact dimensions start finish merge-with]
    "Retrieves a single row histogram for the fact, if merge-with
     not specified histogram time-series is merged with +"))
