(ns unit.crdt-test
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

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
        expected-v "XYZ"]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-union-crdt-basic-ops
  (let [schema (l/union-schema [l/null-schema l/string-schema l/int-schema])
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
              :value 42}]
        expected-v 42]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
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
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(deftest test-nested-map-crdts
  (let [schema (l/map-schema (l/map-schema l/int-schema))
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "k1"
              :key "d"
              :op-type :add-key
              :path []}
             {:add-id "k2"
              :key "w"
              :op-type :add-key
              :path ["d"]}
             {:add-id "a0"
              :op-type :add-value
              :path []
              :value {"d" {"w" 999}}}

             {:add-id "k3"
              :key "a"
              :op-type :add-key
              :path []}
             {:add-id "k4"
              :key "x"
              :op-type :add-key
              :path ["a"]}
             {:add-id "a1"
              :op-type :add-value
              :path ["a" "x"]
              :value 1}

             {:add-id "k5"
              :key "b"
              :op-type :add-key
              :path []}
             {:add-id "k6"
              :key "y"
              :op-type :add-key
              :path ["b"]}
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

             {:add-id "k5"
              :op-type :delete-key
              :path ["b"]}]
        path []
        expected-v {"a" {"x" 42}
                    "d" {"w" 999}}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [ops (repeatedly 500 #(shuffle ops))]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))]
        (is (= expected-v v))))))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(deftest ^:this test-record-crdt-basic-ops
  (let [schema the-rec-schema
        sys-time-ms (u/str->long "1640205282858")
        ops [{:add-id "k1"
              :key :foo/a
              :op-type :add-key
              :path []}
             {:add-id "k2"
              :key :bar/b
              :op-type :add-key
              :path []}
             {:add-id "k3"
              :key :c
              :op-type :add-key
              :path []}
             {:add-id "a1"
              :op-type :add-value
              :path [:foo/a]
              :value 1}
             {:add-id "a2"
              :op-type :add-value
              :path [:bar/b]
              :value "Hi"}
             {:add-id "a2"
              :op-type :delete-value
              :path [:bar/b]}
             {:add-id "a3"
              :op-type :add-value
              :path [:bar/b]
              :value "Yo"}
             {:add-id "a4"
              :op-type :add-value
              :path [:c]
              :value true}
             {:add-id "k3"
              :op-type :delete-key
              :path [:c]}]
        path []
        expected-v {:foo/a 1
                    :bar/b "Yo"}]
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
                                       :path [:foo/a]
                                       :schema schema
                                       :value 1}]
                                :schema schema
                                :sys-time-ms (u/str->long "1640205282858")})
        crdt-2 (crdt/apply-ops {:crdt crdt-1
                                :ops [{:add-id "a2"
                                       :op-type :add-value
                                       :path [:foo/a]
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
              :path []
              :value {"a" {:foo/a 1
                           :bar/b "Hi"
                           :c true}
                      "b" {:foo/a 72
                           :bar/b "there"
                           :c false}}}
             {:add-id "a1"
              :op-type :delete-value
              :path ["a"]}
             {:add-id "a2"
              :op-type :add-value
              :path ["a" :foo/a]
              :value 222}]
        path []
        expected-v {"a" {:foo/a 222}
                    "b" {:foo/a 72
                         :bar/b "there"
                         :c false}}]
    (doseq [ops (combo/permutations ops)]
      (let [crdt (crdt/apply-ops (u/sym-map ops
                                            schema
                                            sys-time-ms))
            v (crdt/get-value (u/sym-map crdt path schema))
            nested-v (crdt/get-value {:crdt crdt
                                      :path ["b" :bar/b]
                                      :schema schema})]
        (is (= expected-v v))
        (is (= "there" nested-v))))))
#_
(deftest test-array-crdt
  (let [schema (l/array-schema l/string-schema)
        ops [{:add-id "a1"
              :op-type :add-value
              :path []
              :value ["A" "B" "C"]}

             ;; Start state: ABC
             (assoc base-op
                    :add-id "a1"
                    :op-type :add-array-node
                    :value "i1")
             (assoc base-op
                    :add-id "a2"
                    :op-type :add-array-node
                    :value "i2")
             (assoc base-op
                    :add-id "a3"
                    :op-type :add-array-node
                    :value "i3")
             (assoc base-op
                    :add-id "a4"
                    :op-type :add-array-edge
                    :value {:head-node-add-id crdt/array-start-add-id
                            :tail-node-add-id "a1"})
             (assoc base-op
                    :add-id "a5"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a1"
                            :tail-node-add-id "a2"})
             (assoc base-op
                    :add-id "a6"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a2"
                            :tail-node-add-id "a3"})
             (assoc base-op
                    :add-id "a7"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a3"
                            :tail-node-add-id crdt/array-end-add-id})
             ;; While offline, client 1 inserts X in between A and B
             ;; Client 1's state is now AXBC
             (assoc base-op
                    :add-id "a8"
                    :op-type :add-array-node
                    :value "i4")
             (assoc base-op
                    :add-id "a5"
                    :op-type :del-array-edge)
             (assoc base-op
                    :add-id "a9"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a1"
                            :tail-node-add-id "a8"})
             (assoc base-op
                    :add-id "a10"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a8"
                            :tail-node-add-id "a2"})
             ;; While offline, client 2 deletes B, then adds Y in its place
             ;; Client 2's state is now AYC
             (assoc base-op
                    :add-id "a2"
                    :op-type :del-array-node)
             (assoc base-op
                    :add-id "a5"
                    :op-type :del-array-edge)
             (assoc base-op
                    :add-id "a6"
                    :op-type :del-array-edge)
             (assoc base-op
                    :add-id "a11"
                    :op-type :add-array-node
                    :value "i5")
             (assoc base-op
                    :add-id "a12"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a1"
                            :tail-node-add-id "a11"})
             (assoc base-op
                    :add-id "a13"
                    :op-type :add-array-edge
                    :value {:head-node-add-id "a11"
                            :tail-node-add-id "a3"})]
        data-store (crdt/apply-ops {:data-store {}
                                    :ops ops})
        crdt-info (u/sym-map data-store item-id schema)
        v (crdt/get-crdt-val crdt-info)
        expected-v ["A" "Y" "X" "C"]
        _ (is (= expected-v v))]))

#_(deftest test-array-indexing
    (let [item-id "a-fine-array"
          schema (l/array-schema l/string-schema)
          sv-base-op {:op-type :add-single-value
                      :schema l/string-schema
                      :sys-time-ms (u/str->long "1640205282858")}
          base-op (u/sym-map item-id schema)
          ops [;; Create the node value CRDTs
               (assoc sv-base-op
                      :add-id "av1"
                      :item-id "i1"
                      :value "A")
               (assoc sv-base-op
                      :add-id "av2"
                      :item-id "i2"
                      :value "B")
               (assoc sv-base-op
                      :add-id "av3"
                      :item-id "i3"
                      :value "C")
               (assoc sv-base-op
                      :add-id "av4"
                      :item-id "i4"
                      :value "X")
               (assoc sv-base-op
                      :add-id "av4"
                      :item-id "i5"
                      :value "Y")

               ;; Start state: ABC
               (assoc base-op
                      :add-id "a1"
                      :op-type :add-array-node
                      :value "i1")
               (assoc base-op
                      :add-id "a2"
                      :op-type :add-array-node
                      :value "i2")
               (assoc base-op
                      :add-id "a3"
                      :op-type :add-array-node
                      :value "i3")
               (assoc base-op
                      :add-id "a4"
                      :op-type :add-array-edge
                      :value {:head-node-add-id crdt/array-start-add-id
                              :tail-node-add-id "a1"})
               (assoc base-op
                      :add-id "a5"
                      :op-type :add-array-edge
                      :value {:head-node-add-id "a1"
                              :tail-node-add-id "a2"})
               (assoc base-op
                      :add-id "a6"
                      :op-type :add-array-edge
                      :value {:head-node-add-id "a2"
                              :tail-node-add-id "a3"})
               (assoc base-op
                      :add-id "a7"
                      :op-type :add-array-edge
                      :value {:head-node-add-id "a3"
                              :tail-node-add-id crdt/array-end-add-id})]
          data-store (crdt/apply-ops {:data-store {}
                                      :ops ops})
          crdt-info (u/sym-map data-store item-id schema)
          v (crdt/get-crdt-val crdt-info)
          _ (is (= ["A" "B" "C"] v))
          kv (crdt/get-crdt-val (assoc crdt-info :k 1))
          _ (is (= "B" kv))]))

#_(deftest test-change-value-of-array-item
    (let [item-id "a-fine-array"
          schema (l/array-schema l/string-schema)
          sv-base-op {:op-type :add-single-value
                      :schema l/string-schema
                      :sys-time-ms (u/str->long "1640205282858")}
          base-op (u/sym-map item-id schema)
          ops [;; Create the node value CRDTs
               (assoc sv-base-op
                      :add-id "av1"
                      :item-id "i1"
                      :value "A")
               (assoc sv-base-op
                      :add-id "av2"
                      :item-id "i2"
                      :value "B")

               ;; Start state: AB
               (assoc base-op
                      :add-id "a1"
                      :op-type :add-array-node
                      :value "i1")
               (assoc base-op
                      :add-id "a2"
                      :op-type :add-array-node
                      :value "i2")
               (assoc base-op
                      :add-id "a4"
                      :op-type :add-array-edge
                      :value {:head-node-add-id crdt/array-start-add-id
                              :tail-node-add-id "a1"})
               (assoc base-op
                      :add-id "a5"
                      :op-type :add-array-edge
                      :value {:head-node-add-id "a1"
                              :tail-node-add-id "a2"})
               (assoc base-op
                      :add-id "a6"
                      :op-type :add-array-edge
                      :value {:head-node-add-id "a2"
                              :tail-node-add-id crdt/array-end-add-id})]
          data-store (crdt/apply-ops {:data-store {}
                                      :ops ops})
          crdt-info (u/sym-map data-store item-id schema)
          v (crdt/get-crdt-val crdt-info)
          _ (is (= ["A" "B"] v))
          change-ops [{:add-id "av2"
                       :item-id "i2"
                       :op-type :del-single-value
                       :sys-time-ms (u/str->long "1640205282858")}
                      {:add-id "xxx"
                       :item-id "i2"
                       :op-type :add-single-value
                       :value "X"
                       :sys-time-ms (u/str->long "1640205282858")}]
          data-store* (crdt/apply-ops {:data-store data-store
                                       :ops change-ops})
          new-v (crdt/get-crdt-val (assoc crdt-info :data-store data-store*))]
      (is (= ["A" "X"] new-v))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(def id-to-pet-owner-schema (l/map-schema pet-owner-schema))
#_
(deftest test-array-of-records
  (let [pet1-item-id "pet1"
        pet2-item-id "pet2"
        array-item-id "array"
        ops [;; Make the 2 pet records
             {:add-id "a1"
              :item-id pet1-item-id
              :k :name
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Bo"}
             {:add-id "a2"
              :item-id pet1-item-id
              :k :species
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Canis familiaris"}
             {:add-id "a3"
              :item-id pet2-item-id
              :k :name
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Sam"}
             {:add-id "a4"
              :item-id pet2-item-id
              :k :species
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Felis catus"}

             ;; Make the array of pet records
             {:add-id "a5"
              :item-id array-item-id
              :op-type :add-array-node
              :schema (l/array-schema pet-schema)
              :value pet1-item-id}
             {:add-id "a6"
              :item-id array-item-id
              :op-type :add-array-node
              :schema (l/array-schema pet-schema)
              :value pet2-item-id}
             {:add-id "a7"
              :item-id array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id crdt/array-start-add-id
                      :tail-node-add-id "a5"}}
             {:add-id "a8"
              :item-id array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id "a5"
                      :tail-node-add-id "a6"}}
             {:add-id "a9"
              :item-id array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id "a6"
                      :tail-node-add-id crdt/array-end-add-id}}]
        annotated-ops (map #(assoc %
                                   :sys-time-ms (u/str->long "1640205282858"))
                           ops)
        data-store (crdt/apply-ops {:data-store {}
                                    :ops annotated-ops})
        crdt-info {:data-store data-store
                   :item-id array-item-id
                   :schema (l/array-schema pet-schema)}
        v (crdt/get-crdt-val crdt-info)
        expected [{:name "Bo"
                   :species "Canis familiaris"}
                  {:name "Sam"
                   :species "Felis catus"}]
        _ (is (= expected v))]))
#_
(deftest test-nesting-with-arrays-maps-and-records
  (let [map-item-id "the-map"
        pet1-item-id "pet1"
        pet2-item-id "pet2"
        alice-item-id "alice-po"
        pets-array-item-id "pets"
        ops [;; Make the 2 pet records
             {:add-id "a1"
              :item-id pet1-item-id
              :k :name
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Bo"}
             {:add-id "a2"
              :item-id pet1-item-id
              :k :species
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Canis familiaris"}
             {:add-id "a3"
              :item-id pet2-item-id
              :k :name
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Sam"}
             {:add-id "a4"
              :item-id pet2-item-id
              :k :species
              :op-type :add-record-key-value
              :schema pet-schema
              :value "Felis catus"}

             ;; Make the array of pet records
             {:add-id "a5"
              :item-id pets-array-item-id
              :op-type :add-array-node
              :schema (l/array-schema pet-schema)
              :value pet1-item-id}
             {:add-id "a6"
              :item-id pets-array-item-id
              :op-type :add-array-node
              :schema (l/array-schema pet-schema)
              :value pet2-item-id}
             {:add-id "a7"
              :item-id pets-array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id crdt/array-start-add-id
                      :tail-node-add-id "a5"}}
             {:add-id "a8"
              :item-id pets-array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id "a5"
                      :tail-node-add-id "a6"}}
             {:add-id "a8.5"
              :item-id pets-array-item-id
              :op-type :add-array-edge
              :schema (l/array-schema pet-schema)
              :value {:head-node-add-id "a6"
                      :tail-node-add-id crdt/array-end-add-id}}

             ;; Make the pet owner record
             {:add-id "a9"
              :item-id alice-item-id
              :k :name
              :op-type :add-record-key-value
              :schema pet-owner-schema
              :value "Alice"}
             {:add-id "a10"
              :item-id alice-item-id
              :k :pets
              :op-type :add-record-key-value
              :schema pet-owner-schema
              :value pets-array-item-id
              :union-branch 1}

             ;; Make the id->pet-owner map
             {:add-id "a11"
              :item-id map-item-id
              :k "alice-id"
              :op-type :add-map-key-value
              :schema id-to-pet-owner-schema
              :value alice-item-id}
             {:add-id "a12"
              :item-id map-item-id
              :k "alice-id"
              :op-type :add-map-key
              :schema id-to-pet-owner-schema}]
        annotated-ops (map #(assoc %
                                   :sys-time-ms (u/str->long "1640205282858"))
                           ops)
        data-store (crdt/apply-ops {:data-store {}
                                    :ops annotated-ops})
        crdt-info {:data-store data-store
                   :item-id map-item-id
                   :schema id-to-pet-owner-schema}
        v (crdt/get-crdt-val crdt-info)
        expected {"alice-id" {:name "Alice",
                              :pets [{:name "Bo"
                                      :species "Canis familiaris"}
                                     {:name "Sam"
                                      :species "Felis catus"}]}}
        _ (is (= expected v))
        kv (crdt/get-crdt-val (assoc crdt-info :k "alice-id"))]
    (is (= (expected "alice-id") kv))))

#_(deftest test-empty-stuff
    (let [item-id "my-str"
          schema l/string-schema
          data-store (crdt/apply-ops {:data-store {}
                                      :ops []})
          crdt-info (u/sym-map item-id schema data-store)
          v (crdt/get-crdt-val crdt-info)
          _ (is (= nil v))]))
