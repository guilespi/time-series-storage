(defproject time-series-storage "0.3.7"
  :description "Time Series Storage Library"
  :url "https://github.com/guilespi/time-series-storage"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.1.4.jre7"]
                 [com.microsoft.sqlserver/sqljdbc4 "4.2"]
                 [clj-time "0.15.0"]
                 [sqlingvo "0.9.31"]]
  :profiles {:1.7.0 {:dependencies [[org.clojure/clojure "1.7.0-RC1"]]}})
