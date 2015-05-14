(ns time-series-storage.query-test
  (:require [clj-time.coerce :as tcoerce]
            [clj-time.core :as t])
  (:use clojure.test
        time-series-storage.query))

(deftest fill-range-collapse-counter-test
  (testing "1 data point trivial timeseries with step"
    (let [start #inst "2015-03-21T09:00:00"
          data-points [{:timestamp (tcoerce/to-string start) :counter 2 :key "some-key"}]]
      (is (= {{:key "some-key"} {#inst "2015-03-21T09:00" 2}}
             (fill-range (collapse data-points :hour)
                         start
                         #inst "2015-03-21T09:40:00"
                         :hour)))))

  (testing "1 data point trivial timeseries without step"
    (let [start #inst "2015-03-21T09:00:00"
          data-points [{:timestamp (tcoerce/to-string start) :counter 2 :key "some-key"}]]
      (is (= {{:key "some-key"} {:all 2}}
             (fill-range (collapse data-points :none)
                         start
                         #inst "2015-03-21T09:40:00"
                         :none)))))

  (testing "many data points timeseries"
    (let [start (tcoerce/from-date #inst "2015-03-21T09:00:00")
          data-points [{:timestamp (tcoerce/to-string start) :counter 2 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 40))) :counter 1 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 80))) :counter 8 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 130))) :counter 4 :key "some-key"}]
          collapsed (collapse data-points :hour)]
      (is (= {{:key "some-key"} {(tcoerce/to-date start) 3
                                 (tcoerce/to-date (t/plus- start (t/hours 1))) 8
                                 (tcoerce/to-date (t/plus- start (t/hours 2))) 4}}
             (fill-range collapsed
                         (tcoerce/to-date start)
                         #inst "2015-03-21T11:40:00"
                         :hour))))))

(deftest fill-range-collapse-average-test
  (testing "1 data point trivial timeseries without step"
    (let [start #inst "2015-03-21T09:00:00"
          step :none
          data-points [{:timestamp (tcoerce/to-string start) :total 20 :counter 2 :key "some-key"}]]
      (is (= {{:key "some-key"} {:all {:total 20 :counter 2}}}
             (fill-range (collapse data-points step)
                         start
                         #inst "2015-03-21T09:40:00"
                         step)))))

  (testing "1 data point trivial timeseries with step"
    (let [start #inst "2015-03-21T09:00:00"
          step :hour
          data-points [{:timestamp (tcoerce/to-string start) :total 20 :counter 2 :key "some-key"}]]
      (is (= {{:key "some-key"} {#inst "2015-03-21T09:00" {:total 20 :counter 2}}}
             (fill-range (collapse data-points step)
                         start
                         #inst "2015-03-21T09:40:00"
                         step)))))

  (testing "many data points timeseries"
    (let [start (tcoerce/from-date #inst "2015-03-21T09:00:00")
          step :hour
          data-points [{:timestamp (tcoerce/to-string start) :total 20 :counter 2 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 40))) :total 30 :counter 1 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 80))) :total 34 :counter 8 :key "some-key"}
                       {:timestamp (tcoerce/to-string (t/plus- start (t/minutes 130))) :total 42 :counter 4 :key "some-key"}
                       ]
          collapsed (collapse data-points step)]
      (is (= {{:key "some-key"} {(tcoerce/to-date start) {:total 50 :counter 3}
                                 (tcoerce/to-date (t/plus- start (t/hours 1))) {:total 34 :counter 8}
                                 (tcoerce/to-date (t/plus- start (t/hours 2))) {:total 42 :counter 4}}}
             (fill-range collapsed
                         (tcoerce/to-date start)
                         #inst "2015-03-21T11:40:00"
                         step))))))
