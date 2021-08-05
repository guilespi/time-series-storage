(defproject time-series-storage "0.3.10"
  :description "Time Series Storage Library"
  :url "https://github.com/guilespi/time-series-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [clj-time "0.8.0"]
                 [sqlingvo "0.7.10"]]
  :profiles {:1.7.0 {:dependencies [[org.clojure/clojure "1.7.0-RC1"]]}})
