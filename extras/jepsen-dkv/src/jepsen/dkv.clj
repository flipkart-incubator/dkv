(ns jepsen.dkv
  (:require 
    [clojure.tools.logging :refer :all]
    [clojure.string :as str]
    [jepsen [cli :as cli]
     [control :as c]
     [db :as db]
     [tests :as tests]]
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
          :-dbFolder        dbDir
          :-dbListenAddr    (client-url node)
          :-dbRole          "master"
          :-nexusNodeId     (-> test :nodes (.indexOf node) inc)
          :-nexusClusterUrl (nexus-url test))

        (Thread/sleep initTime)))

    (teardown! [_ test node]
      (info node "shutting down dkv")
      (c/su
        (cu/stop-daemon! binary pidfile)
        (c/exec :rm :-rf dbDir)))))

(defn dkv-test
  [opts]
  (merge tests/noop-test opts
         {:name "dkv"
          :os debian/os
          :db (db "1.0.0")}))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dkv-test}) 
                   (cli/serve-cmd)) args))
