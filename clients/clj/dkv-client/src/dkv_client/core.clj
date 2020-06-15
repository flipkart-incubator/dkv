(ns dkv-client.core
  (:require [protojure.grpc.client.providers.http2 :as grpc.http2]
            [dkv.serverpb.DKV.client :as cli]
            [clojure.core.async :refer [<!! >!! <! >! go go-loop] :as async]))

(def default-idle-timeout 10000)

(defn connect
  "Establishes a GRPC connection to the DKV server
  listening on the given host and port."
  ([host port]
   (connect host port default-idle-timeout))
  ([host port idle-timeout]
   @(grpc.http2/connect {:uri (format "http://%s:%d" host port) :idle-timeout idle-timeout})))

(defn getValue
  "Retrieves the value(s) associated with the given key(s)."
  ([conn req]
   (let [getReq {:key (if (string? req) (.getBytes req) req) :readConsistency :linearizable}]
     (let [getRes @(cli/Get conn getReq)]
       (if (string? req)
         (->> getRes :value (map char) (apply str))   
         (:value getRes)))))
  ([conn req & more]
   (let [keysReq (if (string? req) (->> (cons req more) (map #(.getBytes %))) (cons req more))]
     (let [multiGetRes @(cli/MultiGet conn {:keys keysReq :readConsistency :linearizable})]
       (if (string? req)
         (map #(->> % (map char) (apply str)) (:values multiGetRes))
         (:values multiGetRes))))))

(defn putKV
  "Creates the association between the given key and value.
  Replaces the existing value with the given value if association
  already exists. Returns the message retrieved in response."
  ([conn keyReq valReq]
   (let [putReq {:key (if (string? keyReq) (.getBytes keyReq) keyReq)
                 :value (if (string? valReq) (.getBytes valReq) valReq)}]
     (let [putRes @(cli/Put conn putReq)]
       (->> putRes :status :message)))))

(defn iterateKV
  "Iterates through the key value associations in no particular
  order. Arguments can be used to control where iteration begins
  as well as the pattern of keys whose associations are returned."
  ([conn startKey] (iterateKV conn startKey nil))
  ([conn startKey keyPrefix]
   (let [resChan (async/chan 1 (map #(select-keys % [:key :value])))
         skIterReq {:startKey (if (string? startKey) (.getBytes startKey) startKey)} 
         kpIterReq {:keyPrefix (if (string? keyPrefix) (.getBytes keyPrefix) keyPrefix)} 
         iterReq (if (nil? keyPrefix) skIterReq (merge skIterReq kpIterReq))]
     (cli/Iterate conn iterReq resChan)
     (delay (take-while some? (repeatedly #(<!! resChan)))))))
