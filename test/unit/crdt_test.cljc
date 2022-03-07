(ns unit.crdt-test
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.crdt :as crdt]
   [com.oncurrent.zeno.crdt.array :as array]
   [com.oncurrent.zeno.crdt.common :as crdt-c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(deftest test-single-value-crdt-basic-ops
  (let [schema l/string-schema
        path []
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a1"
              :op-type :add-value
              :path path
              :value "ABC"}
             {:add-id "a1"
              :op-type :delete-value
              :path path}
             {:add-id "a2"
              :op-type :add-value
              :path path
              :value "XYZ"}]
        crdt (crdt/apply-ops (u/sym-map ops schema sys-time-ms))
        expected-v "XYZ"]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-union-crdt-basic-ops
  (let [schema (l/union-schema [l/null-schema l/string-schema l/int-schema])
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a1"
              :op-type :add-value
              :path [1] ; Union branch 1
              :value "ABC"}
             {:add-id "a1"
              :op-type :delete-value
              :path [1]} ; Union branch is required to handle OOO ops
             {:add-id "a2"
              :op-type :add-value
              :path [2] ; Union branch 2
              :value 42}]
        expected-v 42]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            path [] ; Union branch not used for get-value
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-map-crdt-basic-ops
  (let [schema (l/map-schema l/int-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a1"
              :op-type :add-value
              :path ["a"]
              :value 1}
             {:add-id "a2"
              :op-type :add-value
              :path ["b"]
              :value 2}
             {:add-id "a1"
              :op-type :delete-value
              :path ["a"]}
             {:add-id "a3"
              :op-type :add-value
              :path ["a"]
              :value 42}
             {:add-id "a2"
              :op-type :delete-value
              :path ["b"]}]
        path []
        expected-v {"a" 42}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-nested-map-crdts
  (let [schema (l/map-schema (l/map-schema l/int-schema))
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a0"
              :op-type :add-value
              :path ["d" "w"]
              :value 999}
             {:add-id "a1"
              :op-type :add-value
              :path ["a" "x"]
              :value 1}
             {:add-id "az1"
              :op-type :add-value
              :path ["a" "z"]
              :value 84}
             {:add-id "a2"
              :op-type :add-value
              :path ["b" "y"]
              :value 2}
             {:add-id "a1"
              :op-type :delete-value
              :path ["a" "x"]}
             {:add-id "a3"
              :op-type :add-value
              :path ["a" "x"]
              :value 42}
             {:add-id "a2"
              :op-type :delete-value
              :path ["b" "y"]}]
        path []
        expected-v {"a" {"x" 42
                         "z" 84}
                    "d" {"w" 999}}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-record-crdt-basic-ops
  (let [schema the-rec-schema
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a1"
              :op-type :add-value
              :path [:foo/a 1]
              :value 1}
             {:add-id "a2"
              :op-type :add-value
              :path [:bar/b 1]
              :value "Hi"}
             {:add-id "a2"
              :op-type :delete-value
              :path [:bar/b 1]}
             {:add-id "a3"
              :op-type :add-value
              :path [:bar/b 1]
              :value "Yo"}
             {:add-id "a4"
              :op-type :add-value
              :path [:c 1]
              :value true}
             {:add-id "a4"
              :op-type :delete-value
              :path [:c 1]}]
        path []
        expected-v {:foo/a 1
                    :bar/b "Yo"}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-record-crdt-conflict
  (let [schema the-rec-schema
        crdt-1 (crdt/apply-ops {:ops [{:add-id "a1"
                                       :op-type :add-value
                                       :path [:foo/a 1]
                                       :schema schema
                                       :value 1}]
                                :schema schema
                                :sys-time-ms (u/str->long "1640205282858")})
        crdt-2 (crdt/apply-ops {:crdt crdt-1
                                :ops [{:add-id "a2"
                                       :op-type :add-value
                                       :path [:foo/a 1]
                                       :schema schema
                                       :value 42}]
                                :schema schema
                                :sys-time-ms (u/str->long "1640205282862")})
        v (crdt/get-value {:crdt crdt-2
                           :path []
                           :schema schema})
        expected-v {:foo/a 42}]
    (is (= expected-v v))))

(deftest test-nested-crdts
  (let [schema (l/map-schema the-rec-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a1"
              :op-type :add-value
              :path ["a" :foo/a 1]
              :value 1}
             {:add-id "a1"
              :op-type :delete-value
              :path ["a" :foo/a 1]}
             {:add-id "a2"
              :op-type :add-value
              :path ["a" :foo/a 1]
              :value 222}
             {:add-id "a3"
              :op-type :add-value
              :path ["b" :foo/a 1]
              :value 72}
             {:add-id "a4"
              :op-type :add-value
              :path ["b" :bar/b 1]
              :value "there"}
             {:add-id "a5"
              :op-type :add-value
              :path ["b" :c 1]
              :value false}]
        path []
        expected-v {"a" {:foo/a 222}
                    "b" {:foo/a 72
                         :bar/b "there"
                         :c false}}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))
            nested-v (crdt/get-value {:crdt crdt
                                      :path ["b" :bar/b]
                                      :schema schema})]
        (is (= expected-v v))
        (is (= "there" nested-v))))))

(deftest test-array-crdt-conflict
  (let [schema (l/array-schema l/string-schema)
        sys-time-ms (u/str->long "1640205282858")
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        ops [;; Start state: ABC
             {:add-id "a1"
              :op-type :add-value
              :path ["NodeA"]
              :value "A"}
             {:add-id "a2"
              :op-type :add-value
              :path ["NodeB"]
              :value "B"}
             {:add-id "a3"
              :op-type :add-value
              :path ["NodeC"]
              :value "C"}
             {:add-id "a4"
              :op-type :add-array-edge
              :value {:head-node-id array/array-start-node-id
                      :tail-node-id "NodeA"}}
             {:add-id "a5"
              :op-type :add-array-edge
              :value {:head-node-id "NodeA"
                      :tail-node-id "NodeB"}}
             {:add-id "a6"
              :op-type :add-array-edge
              :value {:head-node-id "NodeB"
                      :tail-node-id "NodeC"}}
             {:add-id "a7"
              :op-type :add-array-edge
              :value {:head-node-id "NodeC"
                      :tail-node-id array/array-end-node-id}}
             ;; While offline, client 1 inserts X in between A and B
             ;; Client 1's state is now AXBC
             {:add-id "a8"
              :op-type :add-value
              :path ["NodeX"]
              :value "X"}
             {:add-id "a5"
              :op-type :delete-array-edge}
             {:add-id "a9"
              :op-type :add-array-edge
              :value {:head-node-id "NodeA"
                      :tail-node-id "NodeX"}}
             {:add-id "a10"
              :op-type :add-array-edge
              :value {:head-node-id "NodeX"
                      :tail-node-id "NodeB"}}
             ;; While offline, client 2 deletes B, then adds Y in its place
             ;; Client 2's state is now AYC
             {:add-id "a2"
              :op-type :delete-value
              :path ["NodeB"]}
             {:add-id "a5"
              :op-type :delete-array-edge}
             {:add-id "a6"
              :op-type :delete-array-edge}
             {:add-id "a11"
              :op-type :add-value
              :path ["NodeY"]
              :value "Y"}
             {:add-id "a12"
              :op-type :add-array-edge
              :value {:head-node-id "NodeA"
                      :tail-node-id "NodeY"}}
             {:add-id "a13"
              :op-type :add-array-edge
              :value {:head-node-id "NodeY"
                      :tail-node-id "NodeC"}}]
        path []
        expected-v ["A" "X" "Y" "C"]]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops schema sys-time-ms))
            v (crdt/get-value (u/sym-map crdt make-id path schema))]
        (is (= expected-v v))))))

(deftest test-array-indexing
  (let [schema (l/array-schema l/string-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [;; Start state: ABC
             {:add-id "a1"
              :op-type :add-value
              :path ["NodeA"]
              :value "A"}
             {:add-id "a2"
              :op-type :add-value
              :path ["NodeB"]
              :value "B"}
             {:add-id "a3"
              :op-type :add-value
              :path ["NodeC"]
              :value "C"}
             {:add-id "a4"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id array/array-start-node-id
                      :tail-node-id "NodeA"}}
             {:add-id "a5"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id "NodeA"
                      :tail-node-id "NodeB"}}
             {:add-id "a6"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id "NodeB"
                      :tail-node-id "NodeC"}}
             {:add-id "a7"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id "NodeC"
                      :tail-node-id array/array-end-node-id}}]
        path []]
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops schema sys-time-ms))
            arg (u/sym-map crdt path schema)
            v (crdt/get-value arg)
            _ (is (= ["A" "B" "C"] v))
            kv (crdt/get-value (assoc arg :path [1]))
            _ (is (= "B" kv))]))))

(deftest test-change-value-of-array-item
  (let [schema (l/array-schema l/string-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [;; Start state: ABC
             {:add-id "a1"
              :op-type :add-value
              :path ["NA"]
              :value "A"}
             {:add-id "a2"
              :op-type :add-value
              :path ["NB"]
              :value "B"}
             {:add-id "a4"
              :op-type :add-array-edge
              :value {:head-node-id array/array-start-node-id
                      :tail-node-id "NA"}}
             {:add-id "a5"
              :op-type :add-array-edge
              :value {:head-node-id "NA"
                      :tail-node-id "NB"}}
             {:add-id "a6"
              :op-type :add-array-edge
              :value {:head-node-id "NB"
                      :tail-node-id array/array-end-node-id}}]
        change-ops [{:add-id "a2"
                     :op-type :delete-value
                     :path ["NB"]}
                    {:add-id "a7"
                     :op-type :add-value
                     :path ["NB"]
                     :value "X"}]
        path []
        apply-ops-arg (u/sym-map ops schema sys-time-ms)]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops apply-ops-arg)
            get-value-arg (u/sym-map crdt path schema)
            v (crdt/get-value get-value-arg)
            _ (is (= ["A" "B"] v))
            crdt* (crdt/apply-ops (assoc apply-ops-arg
                                         :crdt crdt
                                         :ops change-ops))
            new-v (crdt/get-value (assoc get-value-arg :crdt crdt*))]
        (is (= ["A" "X"] new-v))))))

(deftest test-array-of-records
  (let [schema (l/array-schema pet-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [;; Make the 2 pet records
             {:add-id "a1"
              :op-type :add-value
              :path ["Pet1" :name 1]
              :value "Bo"}
             {:add-id "a2"
              :op-type :add-value
              :path ["Pet1" :species 1]
              :value "Canis familiaris"}
             {:add-id "a3"
              :op-type :add-value
              :path ["Pet2" :name 1]
              :value "Sam"}
             {:add-id "a4"
              :op-type :add-value
              :path ["Pet2" :species 1]
              :value "Felis catus"}

             ;; Make the array of pet records
             {:add-id "a7"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id array/array-start-node-id
                      :tail-node-id "Pet1"}}
             {:add-id "a8"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id "Pet1"
                      :tail-node-id "Pet2"}}
             {:add-id "a9"
              :op-type :add-array-edge
              :path []
              :value {:head-node-id "Pet2"
                      :tail-node-id array/array-end-node-id}}]
        path []
        expected [{:name "Bo"
                   :species "Canis familiaris"}
                  {:name "Sam"
                   :species "Felis catus"}]]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected v))))))

(deftest test-nesting-with-arrays-maps-and-records
  (let [schema (l/map-schema pet-owner-schema)
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "a0"
              :op-type :add-value
              :path ["alice-id" :name 1]
              :value "Alice"}
             {:add-id "a1"
              :op-type :add-value
              :path ["alice-id" :pets 1 "Pet1" :name 1]
              :value "Bo"}
             {:add-id "a2"
              :op-type :add-value
              :path ["alice-id" :pets 1 "Pet1" :species 1]
              :value "Canis familiaris"}
             {:add-id "a3"
              :op-type :add-value
              :path ["alice-id" :pets 1 "Pet2" :name 1]
              :value "Sam"}
             {:add-id "a4"
              :op-type :add-value
              :path ["alice-id" :pets 1 "Pet2" :species 1]
              :value "Felis catus"}
             {:add-id "a5"
              :op-type :add-array-edge
              :path ["alice-id" :pets 1]
              :value {:head-node-id array/array-start-node-id
                      :tail-node-id "Pet1"}}
             {:add-id "a6"
              :op-type :add-array-edge
              :path ["alice-id" :pets 1]
              :value {:head-node-id "Pet1"
                      :tail-node-id "Pet2"}}
             {:add-id "a7"
              :op-type :add-array-edge
              :path ["alice-id" :pets 1]
              :value {:head-node-id "Pet2"
                      :tail-node-id array/array-end-node-id}}]
        path []
        expected {"alice-id" {:name "Alice",
                              :pets [{:name "Bo"
                                      :species "Canis familiaris"}
                                     {:name "Sam"
                                      :species "Felis catus"}]}}]
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected v))))))

(deftest test-empty-stuff
  (let [schema l/string-schema
        sys-time-ms (u/str->long "1640205282858")
        ops []
        path []
        crdt (crdt/apply-ops (u/sym-map ops
                                        schema
                                        sys-time-ms))
        v (crdt/get-value (u/sym-map crdt path schema))
        _ (is (= nil v))]))
