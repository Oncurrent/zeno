(ns unit.crdt-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdts :as crdts]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-single-value-crdt-basic-ops
  (let [item-id "my-str"
        schema l/string-schema
        sys-time-ms (u/str->long "1640205282858")
        base-op (u/sym-map item-id sys-time-ms)
        ops [(assoc base-op
                    :add-id "a1"
                    :op-type :add-single-value
                    :value "ABC")
             (assoc base-op
                    :add-id "a1"
                    :op-type :del-single-value)
             (assoc base-op
                    :add-id "a2"
                    :op-type :add-single-value
                    :value "XYZ")]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        expected-ds {:single-value-crdts
                     {"my-str"
                      {:current-add-id-to-value-info
                       {"a2" {:add-id "a2",
                              :sys-time-ms sys-time-ms
                              :value "XYZ"}},
                       :deleted-add-ids #{"a1"}}}}
        _ (is (= expected-ds data-store))
        crdt-info (u/sym-map item-id schema data-store)
        v (crdts/get-crdt-val crdt-info)
        expected-v "XYZ"
        _ (is (= expected-v v))]))

(deftest test-union-crdt-basic-ops
  (let [item-id "my-union"
        schema (l/union-schema [l/null-schema l/string-schema l/int-schema])
        sys-time-ms (u/str->long "1640205282858")
        base-op (u/sym-map item-id sys-time-ms)
        ops [(assoc base-op
                    :add-id "a1"
                    :op-type :add-single-value
                    :value "ABC")
             (assoc base-op
                    :add-id "a1"
                    :op-type :del-single-value)
             (assoc base-op
                    :add-id "a2"
                    :op-type :add-single-value
                    :value 42)]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map item-id schema data-store)
        v (crdts/get-crdt-val crdt-info)
        expected-v 42
        _ (is (= expected-v v))]))

(deftest test-map-crdt-basic-ops
  (let [item-id "my-map"
        schema (l/map-schema l/int-schema)
        sys-time-ms (u/str->long "1640205282858")
        base-op (u/sym-map item-id sys-time-ms)
        ops [(assoc base-op
                    :add-id "a1"
                    :k "a"
                    :op-type :add-map-key-value
                    :value 1)
             (assoc base-op
                    :add-id "a2"
                    :k "a"
                    :op-type :add-map-key)
             (assoc base-op
                    :add-id "a3"
                    :k "b"
                    :op-type :add-map-key-value
                    :value 2)
             (assoc base-op
                    :add-id "a4"
                    :k "b"
                    :op-type :add-map-key)
             (assoc base-op
                    :add-id "a1"
                    :k "a"
                    :op-type :del-map-key-value)
             (assoc base-op
                    :add-id "a5"
                    :k "a"
                    :op-type :add-map-key-value
                    :value 42)
             (assoc base-op
                    :add-id "a3"
                    :op-type :del-map-key-value)
             (assoc base-op
                    :add-id "a4"
                    :op-type :del-map-key)]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map item-id schema data-store)
        v (crdts/get-crdt-val crdt-info)
        expected-v {"a" 42}
        _ (is (= expected-v v))]))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(deftest test-record-crdt-basic-ops
  (let [item-id "for-the-record"
        schema the-rec-schema
        sys-time-ms (u/str->long "1640205282858")
        base-op (u/sym-map item-id schema sys-time-ms)
        ops [(assoc base-op
                    :add-id "a1"
                    :k :foo/a
                    :op-type :add-record-key-value
                    :value 1)
             (assoc base-op
                    :add-id "a2"
                    :k :bar/b
                    :op-type :add-record-key-value
                    :value "Hi")
             (assoc base-op
                    :add-id "a2"
                    :k :bar/b
                    :op-type :del-record-key-value)
             (assoc base-op
                    :add-id "a3"
                    :k :bar/b
                    :op-type :add-record-key-value
                    :value "Yo")
             (assoc base-op
                    :add-id "a4"
                    :k :c
                    :op-type :add-record-key-value
                    :value true)
             (assoc base-op
                    :add-id "a4"
                    :k :c
                    :op-type :del-record-key-value)]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map item-id schema data-store)
        v (crdts/get-crdt-val crdt-info)
        expected-v {:foo/a 1
                    :bar/b "Yo"}
        _ (is (= expected-v v))]))

(deftest test-record-crdt-conflict
  (let [item-id "for-the-record"
        ops [{:add-id "a1"
              :k :foo/a
              :op-type :add-record-key-value
              :item-id item-id
              :schema the-rec-schema
              :sys-time-ms (u/str->long "1640205282858")
              :value 1}
             {:add-id "a2"
              :k :foo/a
              :op-type :add-record-key-value
              :item-id item-id
              :schema the-rec-schema
              :sys-time-ms (u/str->long "1640276685205")
              :value 42}]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info {:data-store data-store
                   :item-id item-id
                   :schema the-rec-schema}
        v (crdts/get-crdt-val crdt-info)
        expected-v {:foo/a 42}
        _ (is (= expected-v v))]))

(deftest test-nested-crdts
  (let [schema (l/map-schema the-rec-schema)
        map-item-id "my-map"
        sys-time-ms (u/str->long "1640205282858")
        base-op (u/sym-map schema sys-time-ms)
        ops [;; rec-a
             (assoc base-op
                    :add-id "a1"
                    :item-id "rec-a"
                    :k :foo/a
                    :op-type :add-record-key-value
                    :value 1)
             (assoc base-op
                    :add-id "a2"
                    :item-id "rec-a"
                    :k :bar/b
                    :op-type :add-record-key-value
                    :value "Hi")
             (assoc base-op
                    :add-id "a3"
                    :item-id "rec-a"
                    :k :c
                    :op-type :add-record-key-value
                    :value true)

             ;; rec-b
             (assoc base-op
                    :add-id "a4"
                    :item-id "rec-b"
                    :k :foo/a
                    :op-type :add-record-key-value
                    :value 72)
             (assoc base-op
                    :add-id "a5"
                    :item-id "rec-b"
                    :k :bar/b
                    :op-type :add-record-key-value
                    :value "there")
             (assoc base-op
                    :add-id "a6"
                    :item-id "rec-b"
                    :k :c
                    :op-type :add-record-key-value
                    :value false)

             ;; map of recs
             (assoc base-op
                    :add-id "a7"
                    :item-id map-item-id
                    :k "a"
                    :op-type :add-map-key-value
                    :value "rec-a")
             (assoc base-op
                    :add-id "a8"
                    :item-id map-item-id
                    :k "a"
                    :op-type :add-map-key)
             (assoc base-op
                    :add-id "a9"
                    :item-id map-item-id
                    :k "b"
                    :op-type :add-map-key-value
                    :value "rec-b")
             (assoc base-op
                    :add-id "a10"
                    :item-id map-item-id
                    :k "b"
                    :op-type :add-map-key)]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info {:data-store data-store
                   :item-id map-item-id
                   :schema schema}
        v (crdts/get-crdt-val crdt-info)
        expected-v {"a" {:foo/a 1
                         :bar/b "Hi"
                         :c true}
                    "b" {:foo/a 72
                         :bar/b "there"
                         :c false}}
        _ (is (= expected-v v))
        mkv (crdts/get-crdt-val (assoc crdt-info
                                       :item-id map-item-id
                                       :k "b"))
        _ (is (= (expected-v "b") mkv))
        rkv (crdts/get-crdt-val (assoc crdt-info
                                       :item-id "rec-b"
                                       :k :bar/b
                                       :schema the-rec-schema))
        _ (is (= "there" rkv))]))

(deftest test-array-crdt
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
                    :value {:head-node-add-id crdts/array-start-add-id
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
                            :tail-node-add-id crdts/array-end-add-id})
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
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map data-store item-id schema)
        v (crdts/get-crdt-val crdt-info)
        expected-v ["A" "Y" "X" "C"]
        _ (is (= expected-v v))]))

(deftest test-array-indexing
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
                    :value {:head-node-add-id crdts/array-start-add-id
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
                            :tail-node-add-id crdts/array-end-add-id})]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map data-store item-id schema)
        v (crdts/get-crdt-val crdt-info)
        _ (is (= ["A" "B" "C"] v))
        kv (crdts/get-crdt-val (assoc crdt-info :k 1))
        _ (is (= "B" kv))]))

(deftest test-change-value-of-array-item
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
                    :value {:head-node-add-id crdts/array-start-add-id
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
                            :tail-node-add-id crdts/array-end-add-id})]
        data-store (crdts/apply-ops {:data-store {}
                                     :ops ops})
        crdt-info (u/sym-map data-store item-id schema)
        v (crdts/get-crdt-val crdt-info)
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
        data-store* (crdts/apply-ops {:data-store data-store
                                      :ops change-ops})
        new-v (crdts/get-crdt-val (assoc crdt-info :data-store data-store*))]
    (is (= ["A" "X"] new-v))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(def id-to-pet-owner-schema (l/map-schema pet-owner-schema))

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
              :value {:head-node-add-id crdts/array-start-add-id
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
                      :tail-node-add-id crdts/array-end-add-id}}]
        annotated-ops (map #(assoc %
                                   :sys-time-ms (u/str->long "1640205282858"))
                           ops)
        data-store (crdts/apply-ops {:data-store {}
                                     :ops annotated-ops})
        crdt-info {:data-store data-store
                   :item-id array-item-id
                   :schema (l/array-schema pet-schema)}
        v (crdts/get-crdt-val crdt-info)
        expected [{:name "Bo"
                   :species "Canis familiaris"}
                  {:name "Sam"
                   :species "Felis catus"}]
        _ (is (= expected v))]))

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
              :value {:head-node-add-id crdts/array-start-add-id
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
                      :tail-node-add-id crdts/array-end-add-id}}

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
        data-store (crdts/apply-ops {:data-store {}
                                     :ops annotated-ops})
        crdt-info {:data-store data-store
                   :item-id map-item-id
                   :schema id-to-pet-owner-schema}
        v (crdts/get-crdt-val crdt-info)
        expected {"alice-id" {:name "Alice",
                              :pets [{:name "Bo"
                                      :species "Canis familiaris"}
                                     {:name "Sam"
                                      :species "Felis catus"}]}}
        _ (is (= expected v))
        kv (crdts/get-crdt-val (assoc crdt-info :k "alice-id"))]
    (is (= (expected "alice-id") kv))))

(deftest test-empty-stuff
  (let [item-id "my-str"
        schema l/string-schema
        data-store (crdts/apply-ops {:data-store {}
                                     :ops []})
        crdt-info (u/sym-map item-id schema data-store)
        v (crdts/get-crdt-val crdt-info)
        _ (is (= nil v))]))
