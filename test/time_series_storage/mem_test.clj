(ns time-series-storage.mem-test
  (:use [clojure.test])
  (:require [time-series-storage.api :as t]
            [clojure.java.jdbc :as j]
            [sqlingvo.core :as sql]
            [sqlingvo.db :as sqdb]
            [time-series-storage.mem :as mem]
            [clj-time.coerce :as tcoerce]))

(def service (mem/make))

(defn init-schema-fixture [f]
  (try
    (t/drop-schema! service)
    (catch Exception e))
  (t/init-schema! service)
  (f)
  )

(use-fixtures :each init-schema-fixture)

(deftest define-fact
  (t/define-fact! service :signups :counter 10 {})
  (is (= [{:type "counter"
           :id "signups"
           :slice 10}]
         (->> service
              t/facts
              (mapv #(select-keys % [:type
                                     :id
                                     :slice]))))))

(deftest define-fact-with-options
  (t/define-fact! service :time-distr :histogram 15 {:name "Time histogram"
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

(deftest define-dimension

  (t/define-dimension! service :company  {:group_only true :name "Compania"})
  (t/define-dimension! service :campaign {:grouped_by [[:company]] :name "Campania"})
  (t/define-dimension! service :channel  {:grouped_by [[:company :campaign]] :name "Canal"})

  (is (= [{:id "company"  :name "Compania" :group_only true  :grouped_by [[]]}
          {:id "campaign" :name "Campania" :group_only false :grouped_by [[:company]]}
          {:id "channel"  :name "Canal"    :group_only false :grouped_by [[:company :campaign]]}]
         (->> service
              t/dimensions
              (map #(select-keys % [:name
                                    :id
                                    :grouped_by
                                    :group_only]))
              reverse))))

(deftest new-fact-and-get-timeseries

    (t/define-fact! service :signups :counter 10 {:name "Cantidad de registros"
                                                  :filler 0
                                                  :units "counter"})

    (t/define-dimension! service :dependency {:name "Dependencia de Correo" :facts [:signups]})
    (t/define-dimension! service :dependency_user {:grouped_by [[:dependency]] :name "Usuario" :facts [:signups]})

    (t/new-fact! service :signups #inst "2014-03-21T09:09" 1 {:dependency "32" :dependency_user "pepe"})
    (t/new-fact! service :signups #inst "2014-03-21T10:23" 1 {:dependency "31" :dependency_user "juanele"})

    (testing "1 timeseries"
      (let [timeseries (t/get-timeseries service
                                         :signups
                                         :dependency_user
                                         {:dependency "31"}
                                         #inst "2014-03-21T09:00"
                                         #inst "2014-03-21T13:00")]
        (is (= {{:dependency_user "juanele" :dependency "31"} {:all 1}}
               timeseries))))

    (testing "many timeseries without step"
      (let [timeseries (t/get-timeseries service
                                         :signups
                                         :dependency_user
                                         {:dependency nil}
                                         #inst "2014-03-21T09:00"
                                         #inst "2014-03-21T13:00")]
        (is (= {{:dependency_user "juanele" :dependency "31"} {:all 1}
                {:dependency_user "pepe" :dependency "32"} {:all 1}}
               timeseries))))


    (testing "many timeseries with step"
      (let [timeseries (t/get-timeseries service
                                         :signups
                                         :dependency_user
                                         {:dependency nil}
                                         #inst "2014-03-21T09:00"
                                         #inst "2014-03-21T13:00"
                                         :hour)]
        (is (= #{{:dependency_user "juanele" :dependency "31"}
                 {:dependency_user "pepe" :dependency "32"}}
               (set (keys timeseries))))
        (is (= {#inst "2014-03-21T09:00" 0
                #inst "2014-03-21T10:00" 1
                #inst "2014-03-21T11:00" 0
                #inst "2014-03-21T12:00" 0}
               (-> timeseries
                   (get {:dependency_user "juanele" :dependency "31"}))))
        ))

)


(deftest new-fact-with-counter-not-1-and-get-timeseries

  (t/define-fact! service :signups :counter 10 {:name "registros" :filler 0})
  (t/define-dimension! service :dependency {:name "Dependencia de Correo" :facts [:signups]})

  ;; pass counter distinct to 1, for example: 3
  (t/new-fact! service :signups #inst "2014-03-21" 3 {:dependency "32"})

  (is (= {{:dependency "32"} {:all 3}}
         (t/get-timeseries service
                           :signups
                           :dependency
                           {}
                           #inst "2012-01-01"
                           #inst "2020-01-01"))))
