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
    )

  (add-dimension [service id options]
    )

  (new-fact [service id value categories]
    )

  (get-timeseries [service fact dimensions start finish]
    )

  (get-histogram [service fact dimensions start finish]
                 [service fact dimensions start finish merge-with]
    ))

(comment

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

  )
