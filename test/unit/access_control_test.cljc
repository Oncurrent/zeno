(ns unit.access-control-test
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.access-control :as ac]
   [oncurrent.zeno.groups :as groups]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest ^:this test-access-control
  (let [group-ops [{:add-id "I1"
                    :op-type :add-value
                    :path [:groups "G1" "A1" :role 1]
                    :value :admin}
                   {:add-id "I1a"
                    :op-type :add-value
                    :path [:groups "G1" "A1" :type 1]
                    :value :actor}
                   {:add-id "I1"
                    :op-type :add-value
                    :path [:groups "G2" "A2" :role 1]
                    :value :admin}
                   {:add-id "I1a"
                    :op-type :add-value
                    :path [:groups "G2" "A2" :type 1]
                    :value :actor}
                   {:add-id "I3"
                    :op-type :add-value
                    :path [:groups "G1" "G2" :role 1]
                    :value :member}
                   {:add-id "I3a"
                    :op-type :add-value
                    :path [:groups "G1" "G2" :type 1]
                    :value :group}]
        expected-groups {"G1" {"A1" {:role :admin
                                     :type :actor}
                               "G2" {:role :member
                                     :type :group}}
                         "G2" {"A2" {:role :admin
                                     :type :actor}}}
        group-store (groups/apply-ops {:ops group-ops})
        _ (is (= expected-groups (groups/get-value {:group-store group-store
                                                    :path [:groups]})))
        ac-ops [{:add-id "C1"
                 :op-type :add-value
                 :path (ac/xf-path [:deals "A" groups/everyone-str])
                 :value :no-access}
                {:add-id "C2"
                 :op-type :add-value
                 :path (ac/xf-path [:deals "A" "G1"])
                 :value :r}
                {:add-id "C3"
                 :op-type :add-value
                 :path (ac/xf-path [:deals "A" "G2"])
                 :value :rw}]
        ac-store (ac/apply-ops {:ops ac-ops})]
    (is (= true (ac/can-read? {:ac-store ac-store
                               :group-store group-store
                               :path [:deals "A"]
                               :subject-id "A1"})))
    (is (= true (ac/can-read? {:ac-store ac-store
                               :group-store group-store
                               :path [:deals "A"]
                               :subject-id "A2"})))
    (is (= true (ac/can-read? {:ac-store ac-store
                               :group-store group-store
                               :path [:deals "A"]
                               :subject-id "G2"})))
    (is (= false (ac/can-read? {:ac-store ac-store
                                :group-store group-store
                                :path [:deals "A"]
                                :subject-id groups/everyone-str})))
    (is (= false (ac/can-read? {:ac-store ac-store
                                :group-store group-store
                                :path [:deals "A"]
                                :subject-id "random"})))

    (is (= false (ac/can-write? {:ac-store ac-store
                                 :group-store group-store
                                 :path [:deals "A"]
                                 :subject-id "A1"})))
    (is (= true (ac/can-write? {:ac-store ac-store
                                :group-store group-store
                                :path [:deals "A"]
                                :subject-id "A2"})))
    (is (= true (ac/can-write? {:ac-store ac-store
                                :group-store group-store
                                :path [:deals "A"]
                                :subject-id "G2"})))
    (is (= false (ac/can-write? {:ac-store ac-store
                                 :group-store group-store
                                 :path [:deals "A"]
                                 :subject-id groups/everyone-str})))
    (is (= false (ac/can-write? {:ac-store ac-store
                                 :group-store group-store
                                 :path [:deals "A"]
                                 :subject-id "random"})))))
