(ns jepsen.dkv
  (:gen-class)
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
     [independent :as independent]
     [nemesis :as nemesis]
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
(def replTimeout 60)

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

(def cli-opts
  "Additional command line options."
  [["-q" "--quorum" "Use quorum reads, instead of reading from any replica."]
   [nil "--qps NUM" "Approximate number of requests per second, per thread."
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  50
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

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
    (let [[k v] (:value op)]
    (try+
      (case (:f op)
        :read (let [value (-> conn (dkvcli/getValue (:quorum test) k) parse-long)]
                (assoc op :type :ok, :value (independent/tuple k value)))
        :write (do (dkvcli/putKV conn k (str v))
                   (assoc op :type :ok)))
      (catch java.lang.Exception ex
        (assoc op :type (if (= :read (:f op)) :fail :info) :error :timeout)))))

  (teardown! [this test])

  (close! [_ test]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn dkv-test
  [opts]
  (let [quorum (boolean (:quorum opts))]
    (merge tests/noop-test opts
           {:name "dkv"
            :quorum quorum
            :os debian/os
            :db (db "1.0.0")
            :client (Client. nil)
            :nemesis (nemesis/partition-random-halves)
            :checker (checker/compose
                       {:perf (checker/perf)
                        :indep (independent/checker
                                 (checker/compose
                                   {:linear (checker/linearizable {:model (model/register) :algorithm :linear})
                                    :timeline (timeline/html)}))})
            :generator (->> (independent/concurrent-generator (:concurrency opts) (range)
                                                              (fn [k] (->>
                                                                        (gen/mix [r w])
                                                                        (gen/stagger (/ (:qps opts)))
                                                                        (gen/limit (:ops-per-key opts)))))
                            (gen/nemesis
                              (gen/seq (cycle [(gen/sleep 5)
                                               {:type :info, :f :start}
                                               (gen/sleep 5)
                                               {:type :info, :f :stop}])))
                            (gen/time-limit (:time-limit opts)))})))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dkv-test :opt-spec cli-opts})
                   (cli/serve-cmd)) args))
