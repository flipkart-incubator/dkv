(ns dkv-client.core-test
  (:require [clojure.test :refer :all]
            [dkv-client.core :refer :all]
            [protojure.grpc.client.api :as grpc.api]))

(def dkvHost "localhost")
(def dkvPort 8080)
(def quorum? true)

(def test-env (atom {}))

(defn create-conn []
  (swap! test-env assoc :conn (connect dkvHost dkvPort)))

(defn close-conn []
  (grpc.api/disconnect (:conn @test-env)))

(defn wrap-test [test-fn]
  (create-conn)
  (test-fn)
  (close-conn))

(use-fixtures :once wrap-test)

(deftest client-test
  (testing "should perform PUT and GET"
    (is (empty? (putKV (:conn @test-env) "cljHello" "cljWorld")))
    (is (= "cljWorld" (getValue (:conn @test-env) quorum? "cljHello"))))

  (testing "should perform MultiGET"
    (let [numKeys 10
          testData (map #(conj [] (str "K_" %) (str "V_" %)) (range 0 numKeys))]
      (doseq [kvPair testData] (is (empty? (putKV (:conn @test-env) (first kvPair) (second kvPair)))))
      (let [actVals (apply getValue (:conn @test-env) quorum? (map first testData))]
        (dotimes [i numKeys] (is (= (nth actVals i) (str "V_" i))))))))
