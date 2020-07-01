(defproject jepsen-dkv "0.1.0-SNAPSHOT"
  :description "Jepsen tests for DKV"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"
            :year 2020
            :key "apache-2.0"}
  :main jepsen.dkv
  :profiles {:default []
             :uberjar {:aot :all}}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.1.13"]
                 [org.eclipse.jetty.http2/http2-client "9.4.28.v20200408"]
                 [dkv-client "0.1.0-SNAPSHOT"]])
