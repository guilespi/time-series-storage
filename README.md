time-series-storage
-

A Clojure library designed to store time-series oriented metrics.

Main idea is to provide a standard API to track and query counters,
histograms and other metrics, independently of the storage used.

Support for `Postgres` and `Cassandra` will be added.

Intended to be useful as a side-companion to Datomic if you need to
keep track of mutable trends using the same backend.

Concepts
-

####Facts

Anything you need to keep track of, count or aggregate.

Can be discrete events such as `conversions` or `visits`, or events
with a magnitude, such as `conversion-time`.


##Dimensions

Any category you want your facts to be categorized, grouped or
filtered by.

Usually named values such as `country`, `operating-system`,
`referral`, etc.

Usage
-

Work in progress. should be moved to the protocol api samples

```clojure

(def db-spec (or (System/getenv "DATABASE_URL")
  "postgresql://postgres:postgres@localhost:5432/timeseries"))

(require '[time-series-storage.api :as api])
(require '[time-series-storage.postgres :as p])

(import '[time_series_storage.postgres Postgres])

(def service (Postgres. db-spec))

(t/define-fact service ...)

```

```clojure

  (api/init-schema! service)

  ;;create-counter
  (api/define-fact! service :registros :counter 15 {:name "Cantidad de registros"
                                                :filler 0
                                                :units "counter"})

  ;;create-average
  (api/define-fact! service :avg_time :average 15 {:name "Tiempo promedio"
                                               :filler 0
                                               :units "seconds"})
  ;;create-histogram
  (api/define-fact! service :time-distr :histogram 15 {:name "Histograma de tiempo"
                                                   :filler 0
                                                   :units "seconds"
                                                   :start 0
                                                   :end 1000
                                                   :step 100})

  ;;
  (api/define-dimension! service :company {:group_only true :name "Compania"})
  (api/define-dimension! service :campaign {:grouped_by [[:company]] :name "Campania"})
  (api/define-dimension! service :channel {:grouped_by [[:company :campaign]] :name "Canal"})

  (api/define-dimension! service :dependency {:name "Dependencia de Correo"})
  (api/define-dimension! service :dependency_user {:grouped_by [[:dependency]] :name "Usuario"})

  (api/new-fact! service :registros 1 {:dependency "32" :dependency_user "juanele"})
  (api/new-fact! service :registros 2 {:dependency "35" :dependency_user "pepe"})

  (api/get-timeseries service
         :registros
         :dependency
         {:dependency "32"}
         #inst "2012-01-01"
         #inst "2020-01-01")
  ;; => ({:counter 1, :dependency "32", :timestamp "20141121T134500.000Z"})

  (api/get-timeseries service
           :registros
           :dependency
           {:dependency "35"}
           #inst "2012-01-01"
           #inst "2020-01-01")
  ;; => ({:counter 2, :dependency "35", :timestamp "20141121T134500.000Z"}

  (api/get-timeseries service
           :registros
           :dependency
           {}
           #inst "2012-01-01"
           #inst "2020-01-01")
  ;; => ({:counter 1, :dependency "32", :timestamp "20141121T134500.000Z"} {:counter 2, :dependency "35", :timestamp "20141121T134500.000Z"})

  (api/new-fact! service :avg_time 15 {:company "bbva" :campaign "ventas" :channel "web"})
  (api/new-fact! service :avg_time 15 {:company "bbva" :campaign "ventas" :channel "mail"})

  (api/get-timeseries service
         :avg_time
         :campaign
         {:company "bbva" :campaign "ventas"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (api/get-timeseries service
         :avg_time
         :channel
         {:company "bbva" :campaign "ventas" :channel "web"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

```

License
----

Copyright © 2014 Guillermo Winkler

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
