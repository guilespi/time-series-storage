(ns time-series-storage.postgres-test
  (:use [clojure.test])
  (:require [time-series-storage.postgres :as p]
            [time-series-storage.api :as t])
  (:import [time_series_storage.postgres Postgres]))

(def db-spec (or (System/getenv "DATABASE_URL")
  "postgresql://postgres:postgres@localhost:5432/timeseries_test"))

(def service (Postgres. db-spec))

(defn init-schema [service]
  (t/drop-schema! service)
  (t/init-schema! service)
  )

(deftest postgres-time-series
  (testing "add-fact"

    (init-schema service)
    (t/add-fact! service :signups :counter 10 {})

    (is (= [{:type "counter"
             :id "signups"
             :slice 10}]
           (->> service
                t/facts
                (map #(select-keys % [:type
                                      :id
                                      :slice])))
           ))

    )

  (testing "add-fact with options"

    (init-schema service)
    (t/add-fact! service :time-distr :histogram 15 {:name "Time histogram"
                                                    :filler 0
                                                    :units "seconds"
                                                    :start 0
                                                    :end 1000
                                                    :step 100} )

    (is (= [{:type "histogram"
             :id "time-distr"
             :slice 15
             :name "Time histogram"
             :filler 0
             :units "seconds"
             :start 0
             :end 1000
             :step 100}]
           (->> service
                t/facts
                (map #(select-keys % [:type
                                      :id
                                      :slice
                                      :name
                                      :filler
                                      :units
                                      :start
                                      :end
                                      :step])))
           ))


    )

  (testing "dimension"

    (init-schema service)
    (t/add-dimension! service :company  {:group_only true                 :name "Compania"})
    (t/add-dimension! service :campaign {:grouped_by [:company]           :name "Campania"})
    (t/add-dimension! service :channel  {:grouped_by [:company :campaign] :name "Canal"})

    (is (= [{:id "company"  :name "Compania" :group_only true  :grouped_by [[]]}
            {:id "campaign" :name "Campania" :group_only false :grouped_by [:company]}
            {:id "channel"  :name "Canal"    :group_only false :grouped_by [:company :campaign]}]
           (->> service
                t/dimensions
                (map #(select-keys % [:name
                                      :id
                                      :grouped_by
                                      :group_only])))))

    )

  )
