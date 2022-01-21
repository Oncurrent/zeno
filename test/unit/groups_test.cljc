(ns unit.groups-test
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.test :as t :refer [deftest is]]
   [oncurrent.zeno.groups :as groups]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-group-basic-ops
  (let [group-id "G1"
        actor-id-1 "A1"
        actor-id-2 "A2"
        actor-id-3 "A3"
        ops [{:add-id "I1"
              :op-type :add-value
              :path [:groups group-id actor-id-1 :role 1]
              :value :admin}
             {:add-id "I1a"
              :op-type :add-value
              :path [:groups group-id actor-id-1 :type 1]
              :value :actor}
             {:add-id "I2"
              :op-type :add-value
              :path [:groups group-id actor-id-2 :role 1]
              :value :member}
             {:add-id "I2a"
              :op-type :add-value
              :path [:groups group-id actor-id-2 :type 1]
              :value :actor}
             {:add-id "I3"
              :op-type :add-value
              :path [:groups group-id actor-id-3 :role 1]
              :value :member}
             {:add-id "I3a"
              :op-type :add-value
              :path [:groups group-id actor-id-3 :type 1]
              :value :actor}
             {:add-id "I2"
              :op-type :delete-value
              :path [:groups group-id actor-id-2 :role 1]}
             {:add-id "I2a"
              :op-type :delete-value
              :path [:groups group-id actor-id-2 :type 1]}
             {:add-id "I3"
              :op-type :delete-value
              :path [:groups group-id actor-id-3 :role 1]}
             {:add-id "I4"
              :op-type :add-value
              :path [:groups group-id actor-id-3 :role 1]
              :value :admin}]
        path [:groups]
        expected {"G1" {"A1" {:role :admin
                              :type :actor}
                        "A3" {:role :admin
                              :type :actor}}}
        group-store (groups/apply-ops (u/sym-map ops))
        groups (groups/get-value (u/sym-map group-store path))]
    (is (= expected groups))))

(deftest ^:this test-nesting
  (let [ops [{:add-id "I1"
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
        path [:groups]
        expected {"G1" {"A1" {:role :admin
                              :type :actor}
                        "G2" {:role :member
                              :type :group}}
                  "G2" {"A2" {:role :admin
                              :type :actor}}}
        group-store (groups/apply-ops (u/sym-map ops))
        groups (groups/get-value (u/sym-map group-store path))]
    (is (= expected groups))
    (is (= true (groups/direct-member? {:group-id "G1"
                                        :group-store group-store
                                        :subject-id "A1"})))
    (is (= true (groups/member? {:group-id "G1"
                                 :group-store group-store
                                 :subject-id "A1"})))
    (is (= false (groups/direct-member? {:group-id "G1"
                                         :group-store group-store
                                         :subject-id "A2"})))
    (is (= true (groups/member? {:group-id "G1"
                                 :group-store group-store
                                 :subject-id "A2"})))
    (is (= false (groups/direct-member? {:group-id "G1"
                                         :group-store group-store
                                         :subject-id "A2"})))
    (is (= true (groups/direct-member? {:group-id "G2"
                                        :group-store group-store
                                        :subject-id "A2"})))
    (is (= true (groups/direct-member? {:group-id "G1"
                                        :group-store group-store
                                        :subject-id "G2"})))
    (is (= true (groups/member? {:group-id "G2"
                                 :group-store group-store
                                 :subject-id "A2"})))
    (is (= true (groups/member? {:group-id groups/everyone-str
                                 :group-store group-store
                                 :subject-id "A2"})))
    (is (= true (groups/member? {:group-id groups/everyone-str
                                 :group-store group-store
                                 :subject-id "random-sid"})))))
