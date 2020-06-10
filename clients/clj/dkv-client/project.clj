(defproject dkv-client "0.1.0-SNAPSHOT"
  :description "Official DKV Client in Clojure"
  :url "https://github.com/flipkart-incubator/dkv"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"
            :year 2020
            :key "apache-2.0"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                   ;; -- PROTOC-GEN-CLOJURE --
                   [org.ow2.asm/asm "7.0"]
                 [protojure "1.2.0"]
                 [protojure/google.protobuf "0.9.1"]
                 [com.google.protobuf/protobuf-java "3.11.1"]
                 ;; -- PROTOC-GEN-CLOJURE HTTP/2 Client Lib Dependency --
                 [org.eclipse.jetty.http2/http2-client "9.4.17.v20190418"]]
  )
