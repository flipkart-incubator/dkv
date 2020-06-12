(ns jepsen.dkv
  (:require 
    [clojure.tools.logging :refer :all]
    [clojure.string :as str]
    [dkv-client.core :as dkvcli]
    [jepsen [cli :as cli]
     [checker :as checker]
     [control :as c]
     [client :as client]
     [db :as db]
     [generator :as gen]
     [tests :as tests]]
    [knossos.model :as model]
    [slingshot.slingshot :refer [try+]]
    [jepsen.checker.timeline :as timeline]
    [jepsen.control.util :as cu]
    [jepsen.os.debian :as debian]))

(def peer-port 9020)
(def client-port 9081)
(def dir "/opt/dkv")
(def dbDir "/tmp/dkvsrv")
(def binary "dkvsrv")
(def logfile (str dir "/dkvsrv.log"))
(def pidfile (str dir "/dkvsrv.pid"))
(def initTime 10000)
(def replTimeout 30)

(defn node-url
  ([node port] (str (name node) ":" port))
  ([proto node port] (str proto "://" (name node) ":" port)))

(defn peer-url
  [node]
  (node-url "http" node peer-port))

(defn client-url
  [node]
  (node-url node client-port))

(defn nexus-url
  [test]
  (subs (reduce (fn [acc node] (str acc "," (peer-url node))) "" (:nodes test)) 1))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when (and s (not (empty? (.trim s)))) (Long/parseLong (.trim s))))

(defn db
  "DKV DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "starting dkv" version)
      (c/su
        ;DKV launcher creates the `dbDir` automatically if missing
        ;(c/exec :mkdir :-p dbDir)
        ;(c/exec :chmod :a+x :-R dbDir)
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :-dbFolder         dbDir
          :-dbListenAddr     (client-url node)
          :-dbRole           "master"
          :-nexusNodeId      (-> test :nodes (.indexOf node) inc)
          :-nexusReplTimeout replTimeout
          :-nexusClusterUrl  (nexus-url test))

        (Thread/sleep initTime)))

    (teardown! [_ test node]
      (info node "shutting down dkv")
      (c/su
        (cu/stop-daemon! binary pidfile)
        (c/exec :rm :-rf dbDir)))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (dkvcli/connect node client-port)))

  (setup! [this test])

  (invoke! [_ test op]
    (case (:f op)
      :read (assoc op :type :ok, :value (parse-long (dkvcli/getValue conn "jepsen_key")))
      :write (do (dkvcli/putKV conn "jepsen_key" (str (:value op))) (assoc op :type :ok))))

  (teardown! [this test])

  (close! [_ test]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn dkv-test
  [opts]
  (merge tests/noop-test opts
         {:name "dkv"
          :os debian/os
          :db (db "1.0.0")
          :client (Client. nil)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear (checker/linearizable {:model     (model/register)
                                                     :algorithm :linear})
                      :timeline (timeline/html)})
          :generator (->> (gen/mix [r w])
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 15))}))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dkv-test}) 
                   (cli/serve-cmd)) args))
