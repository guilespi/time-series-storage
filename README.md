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
;;create-counter
  (schema/create-fact! db-spec :registros :counter 15 {:name "Cantidad de registros"
                                                :filler 0
                                                :units "counter"})

  ;;create-average
  (schema/create-fact! db-spec :avg_time :average 15 {:name "Tiempo promedio"
                                               :filler 0
                                               :units "seconds"})
  ;;create-histogram
  (schema/create-fact! db-spec :time-distr :histogram 15 {:name "Histograma de tiempo"
                                                   :filler 0
                                                   :units "seconds"
                                                   :start 0
                                                   :end 1000
                                                   :step 100})

  ;;
  (schema/create-dimension! db-spec :company {:group_only true :name "Compania"})
  (schema/create-dimension! db-spec :campaign {:grouped_by [:company] :name "Campania"})
  (schema/create-dimension! db-spec :channel {:grouped_by [:company :campaign] :name "Canal"})

  (schema/create-dimension! db-spec :dependency {:name "Dependencia de Correo"})
  (schema/create-dimension! db-spec :dependency_user {:grouped_by [:dependency] :name "Usuario"})

  (u/new-fact db-spec :registros 1 {:dependency "32" :dependency_user "juanele"})
  (u/new-fact db-spec :avg_time 15 {:company "bbva" :campaign "ventas" :channel "web"})
  (u/new-fact db-spec :avg_time 15 {:company "bbva" :campaign "ventas" :channel "mail"})

  (q/query db-spec
         (schema/get-fact db-spec :avg_time)
         (schema/get-dimension db-spec :campaign)
         {:company "bbva" :campaign "ventas"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (q/query db-spec
         (schema/get-fact db-spec :avg_time)
         (schema/get-dimension db-spec :channel)
         {:company "bbva" :campaign "ventas" :channel "web"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (q/query db-spec
         (schema/get-fact db-spec :registros)
         (schema/get-dimension db-spec :dependency)
         {:dependency "32"}
         (tcoerce/from-date #inst "2012-01-01")
         (tcoerce/from-date #inst "2020-01-01"))

  (q/query db-spec
           (schema/get-fact db-spec :registros)
           (schema/get-dimension db-spec :dependency)
           {:dependency "35"}
           (tcoerce/from-date #inst "2012-01-01")
           (tcoerce/from-date #inst "2020-01-01"))
```

License
----

Copyright © 2014 Guillermo Winkler

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
