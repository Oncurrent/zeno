(ns unit.crdt-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdts :as crdts]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-single-value-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %)
           item-id "my-str"
           schema l/string-schema
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id sys-time-ms)
           ops [(assoc base-op
                       :add-id "a1"
                       :op-type :add-single-value
                       :serialized-value (au/<? (<ser-str "ABC")))
                (assoc base-op
                       :add-id "a1"
                       :op-type :del-single-value)
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-single-value
                       :serialized-value (au/<? (<ser-str "XYZ")))]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v "XYZ"
           _ (is (= expected-v v))]))))

(deftest test-union-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser #(storage/<value->serialized-value storage %1 %2)
           item-id "my-union"
           schema (l/union-schema [l/null-schema l/string-schema l/int-schema])
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id sys-time-ms)
           ops [(assoc base-op
                       :add-id "a1"
                       :op-type :add-single-value
                       :serialized-value (au/<? (<ser l/string-schema "ABC")))
                (assoc base-op
                       :add-id "a1"
                       :op-type :del-single-value)
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-single-value
                       :serialized-value (au/<? (<ser l/int-schema 42)))]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v 42
           _ (is (= expected-v v))]))))

(deftest test-map-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser-int #(storage/<value->serialized-value storage l/int-schema %)
           item-id "my-map"
           schema (l/map-schema l/int-schema)
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id sys-time-ms)
           ops [(assoc base-op
                       :add-id "a1"
                       :k "a"
                       :op-type :add-map-key-value
                       :serialized-value (au/<? (<ser-int 1)))
                (assoc base-op
                       :add-id "a2"
                       :k "a"
                       :op-type :add-map-key)
                (assoc base-op
                       :add-id "a3"
                       :k "b"
                       :op-type :add-map-key-value
                       :serialized-value (au/<? (<ser-int 2)))
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
                       :serialized-value (au/<? (<ser-int 42)))
                (assoc base-op
                       :add-id "a3"
                       :op-type :del-map-key-value)
                (assoc base-op
                       :add-id "a4"
                       :op-type :del-map-key)]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v {"a" 42}
           _ (is (= expected-v v))]))))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(deftest test-record-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser #(storage/<value->serialized-value storage %1 %2)
           item-id "for-the-record"
           schema the-rec-schema
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id schema sys-time-ms)
           ops [(assoc base-op
                       :add-id "a1"
                       :k :foo/a
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/int-schema 1)))
                (assoc base-op
                       :add-id "a2"
                       :k :bar/b
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/string-schema "hi")))
                (assoc base-op
                       :add-id "a2"
                       :k :bar/b
                       :op-type :del-record-key-value)
                (assoc base-op
                       :add-id "a3"
                       :k :bar/b
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/string-schema "Yo")))
                (assoc base-op
                       :add-id "a4"
                       :k :c
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/boolean-schema true)))
                (assoc base-op
                       :add-id "a4"
                       :k :c
                       :op-type :del-record-key-value)]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v {:foo/a 1
                       :bar/b "Yo"}
           _ (is (= expected-v v))]))))

(deftest test-record-crdt-conflict
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser #(storage/<value->serialized-value storage %1 %2)
           item-id "for-the-record"
           ops [{:add-id "a1"
                 :k :foo/a
                 :op-type :add-record-key-value
                 :item-id item-id
                 :schema the-rec-schema
                 :serialized-value (au/<? (<ser l/int-schema 1))
                 :sys-time-ms (u/str->long "1640205282858")}
                {:add-id "a2"
                 :k :foo/a
                 :op-type :add-record-key-value
                 :item-id item-id
                 :schema the-rec-schema
                 :serialized-value (au/<? (<ser l/int-schema 42))
                 :sys-time-ms (u/str->long "1640276685205")}]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info {:item-id item-id
                      :schema the-rec-schema
                      :storage storage}
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v {:foo/a 42}
           _ (is (= expected-v v))]))))

(deftest test-nested-crdts
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser #(storage/<value->serialized-value storage %1 %2)
           schema (l/map-schema the-rec-schema)
           map-item-id "my-map"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map schema sys-time-ms)
           ops [;; rec-a
                (assoc base-op
                       :add-id "a1"
                       :item-id "rec-a"
                       :k :foo/a
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/int-schema 1)))
                (assoc base-op
                       :add-id "a2"
                       :item-id "rec-a"
                       :k :bar/b
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/string-schema "hi")))
                (assoc base-op
                       :add-id "a3"
                       :item-id "rec-a"
                       :k :c
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/boolean-schema true)))

                ;; rec-b
                (assoc base-op
                       :add-id "a4"
                       :item-id "rec-b"
                       :k :foo/a
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/int-schema 72)))
                (assoc base-op
                       :add-id "a5"
                       :item-id "rec-b"
                       :k :bar/b
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/string-schema "there")))
                (assoc base-op
                       :add-id "a6"
                       :item-id "rec-b"
                       :k :c
                       :op-type :add-record-key-value
                       :serialized-value (au/<? (<ser l/boolean-schema false)))

                ;; map of recs
                (assoc base-op
                       :add-id "a7"
                       :item-id map-item-id
                       :k "a"
                       :op-type :add-map-key-value
                       :serialized-value (au/<? (<ser l/string-schema "rec-a")))
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
                       :serialized-value (au/<? (<ser l/string-schema "rec-b")))
                (assoc base-op
                       :add-id "a10"
                       :item-id map-item-id
                       :k "b"
                       :op-type :add-map-key)]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map schema storage)
           v (au/<? (crdts/<get-crdt-val
                     (assoc crdt-info :item-id map-item-id)))
           expected-v {"a" {:foo/a 1
                            :bar/b "hi"
                            :c true}
                       "b" {:foo/a 72
                            :bar/b "there"
                            :c false}}
           _ (is (= expected-v v))
           mkv (au/<? (crdts/<get-crdt-val
                       (assoc crdt-info :item-id map-item-id :k "b")))
           _ (is (= (expected-v "b") mkv))
           rkv (au/<? (crdts/<get-crdt-val
                       (assoc crdt-info
                              :item-id "rec-b"
                              :k :bar/b
                              :schema the-rec-schema)))
           _ (is (= "there" rkv))]))))

(deftest test-array-crdt
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           item-id "a-fine-array"
           schema (l/array-schema l/string-schema)
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %1)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           sv-base-op {:op-type :add-single-value
                       :schema l/string-schema
                       :sys-time-ms (u/str->long "1640205282858")}
           base-op (u/sym-map item-id schema)
           ops [;; Create the node value CRDTs
                (assoc sv-base-op
                       :add-id "av1"
                       :item-id "i1"
                       :serialized-value (au/<? (<ser-str "A")))
                (assoc sv-base-op
                       :add-id "av2"
                       :item-id "i2"
                       :serialized-value (au/<? (<ser-str "B")))
                (assoc sv-base-op
                       :add-id "av3"
                       :item-id "i3"
                       :serialized-value (au/<? (<ser-str "C")))
                (assoc sv-base-op
                       :add-id "av4"
                       :item-id "i4"
                       :serialized-value (au/<? (<ser-str "X")))
                (assoc sv-base-op
                       :add-id "av4"
                       :item-id "i5"
                       :serialized-value (au/<? (<ser-str "Y")))

                ;; Start state: ABC
                (assoc base-op
                       :add-id "a1"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i1")))
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i2")))
                (assoc base-op
                       :add-id "a3"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i3")))
                (assoc base-op
                       :add-id "a4"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge crdts/array-start-add-id
                                                     "a1")))
                (assoc base-op
                       :add-id "a5"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a1" "a2")))
                (assoc base-op
                       :add-id "a6"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a2" "a3")))
                (assoc base-op
                       :add-id "a7"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge "a3"
                                                     crdts/array-end-add-id)))
                ;; While offline, client 1 inserts X in between A and B
                ;; Their state is now AXBC
                (assoc base-op
                       :add-id "a8"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i4")))
                (assoc base-op
                       :add-id "a5"
                       :op-type :del-array-edge)
                (assoc base-op
                       :add-id "a9"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a1" "a8")))
                (assoc base-op
                       :add-id "a10"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a8" "a2")))
                ;; While offline, client 2 deletes B, then adds Y in its place
                ;; Their state is now AYC
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
                       :serialized-value (au/<? (<ser-str "i5")))
                (assoc base-op
                       :add-id "a12"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a1" "a11")))
                (assoc base-op
                       :add-id "a13"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a11" "a3")))]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v ["A" "Y" "X" "C"]
           _ (is (= expected-v v))]))))

(deftest test-array-indexing
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           item-id "a-fine-array"
           schema (l/array-schema l/string-schema)
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %1)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           sv-base-op {:op-type :add-single-value
                       :schema l/string-schema
                       :sys-time-ms (u/str->long "1640205282858")}
           base-op (u/sym-map item-id schema)
           ops [;; Create the node value CRDTs
                (assoc sv-base-op
                       :add-id "av1"
                       :item-id "i1"
                       :serialized-value (au/<? (<ser-str "A")))
                (assoc sv-base-op
                       :add-id "av2"
                       :item-id "i2"
                       :serialized-value (au/<? (<ser-str "B")))
                (assoc sv-base-op
                       :add-id "av3"
                       :item-id "i3"
                       :serialized-value (au/<? (<ser-str "C")))
                (assoc sv-base-op
                       :add-id "av4"
                       :item-id "i4"
                       :serialized-value (au/<? (<ser-str "X")))
                (assoc sv-base-op
                       :add-id "av4"
                       :item-id "i5"
                       :serialized-value (au/<? (<ser-str "Y")))

                ;; Start state: ABC
                (assoc base-op
                       :add-id "a1"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i1")))
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i2")))
                (assoc base-op
                       :add-id "a3"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i3")))
                (assoc base-op
                       :add-id "a4"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge crdts/array-start-add-id
                                                     "a1")))
                (assoc base-op
                       :add-id "a5"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a1" "a2")))
                (assoc base-op
                       :add-id "a6"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a2" "a3")))
                (assoc base-op
                       :add-id "a7"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge "a3"
                                                     crdts/array-end-add-id)))]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           _ (is (= ["A" "B" "C"] v))
           kv (au/<? (crdts/<get-crdt-val (assoc crdt-info :k 1)))
           _ (is (= "B" kv))]))))

(deftest test-change-value-of-array-item
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           item-id "a-fine-array"
           schema (l/array-schema l/string-schema)
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %1)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           sv-base-op {:op-type :add-single-value
                       :schema l/string-schema
                       :sys-time-ms (u/str->long "1640205282858")}
           base-op (u/sym-map item-id schema)
           ops [;; Create the node value CRDTs
                (assoc sv-base-op
                       :add-id "av1"
                       :item-id "i1"
                       :serialized-value (au/<? (<ser-str "A")))
                (assoc sv-base-op
                       :add-id "av2"
                       :item-id "i2"
                       :serialized-value (au/<? (<ser-str "B")))

                ;; Start state: AB
                (assoc base-op
                       :add-id "a1"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i1")))
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-str "i2")))
                (assoc base-op
                       :add-id "a4"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge crdts/array-start-add-id
                                                     "a1")))
                (assoc base-op
                       :add-id "a5"
                       :op-type :add-array-edge
                       :serialized-value (au/<? (<ser-edge "a1" "a2")))
                (assoc base-op
                       :add-id "a6"
                       :op-type :add-array-edge
                       :serialized-value (au/<?
                                          (<ser-edge "a2"
                                                     crdts/array-end-add-id)))]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           _ (is (= ["A" "B"] v))
           change-ops [{:add-id "av2"
                        :item-id "i2"
                        :op-type :del-single-value
                        :sys-time-ms (u/str->long "1640205282858")}
                       {:add-id "xxx"
                        :item-id "i2"
                        :op-type :add-single-value
                        :serialized-value (au/<? (<ser-str "X"))
                        :sys-time-ms (u/str->long "1640205282858")}]
           _ (is (= true (au/<? (crdts/<apply-ops! {:ops change-ops
                                                    :storage storage}))))
           new-v (au/<? (crdts/<get-crdt-val crdt-info))]
       (is (= ["A" "X"] new-v))))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(def id-to-pet-owner-schema (l/map-schema pet-owner-schema))

(deftest test-array-of-records
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           pet1-item-id "pet1"
           pet2-item-id "pet2"
           array-item-id "array"
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           ops [;; Make the 2 pet records
                {:add-id "a1"
                 :item-id pet1-item-id
                 :k :name
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Bo"))}
                {:add-id "a2"
                 :item-id pet1-item-id
                 :k :species
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Canis familiaris"))}
                {:add-id "a3"
                 :item-id pet2-item-id
                 :k :name
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Sam"))}
                {:add-id "a4"
                 :item-id pet2-item-id
                 :k :species
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Felis catus"))}

                ;; Make the array of pet records
                {:add-id "a5"
                 :item-id array-item-id
                 :op-type :add-array-node
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-str pet1-item-id))}
                {:add-id "a6"
                 :item-id array-item-id
                 :op-type :add-array-node
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-str pet2-item-id))}
                {:add-id "a7"
                 :item-id array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge crdts/array-start-add-id
                                                     "a5"))}
                {:add-id "a8"
                 :item-id array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge "a5" "a6"))}
                {:add-id "a9"
                 :item-id array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge "a6"
                                                     crdts/array-end-add-id))}]
           annotated-ops (map #(assoc %
                                      :sys-time-ms (u/str->long "1640205282858"))
                              ops)
           _ (is (= true (au/<? (crdts/<apply-ops!
                                 {:ops annotated-ops
                                  :storage storage}))))
           crdt-info {:item-id array-item-id
                      :schema (l/array-schema pet-schema)
                      :storage storage}
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected [{:name "Bo"
                      :species "Canis familiaris"}
                     {:name "Sam"
                      :species "Felis catus"}]
           _ (is (= expected v))]))))

(deftest test-nesting-with-arrays-maps-and-records
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           map-item-id "the-map"
           pet1-item-id "pet1"
           pet2-item-id "pet2"
           alice-item-id "alice-po"
           pets-array-item-id "pets"
           <ser-str #(storage/<value->serialized-value
                      storage l/string-schema %)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           ops [;; Make the 2 pet records
                {:add-id "a1"
                 :item-id pet1-item-id
                 :k :name
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Bo"))}
                {:add-id "a2"
                 :item-id pet1-item-id
                 :k :species
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Canis familiaris"))}
                {:add-id "a3"
                 :item-id pet2-item-id
                 :k :name
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Sam"))}
                {:add-id "a4"
                 :item-id pet2-item-id
                 :k :species
                 :op-type :add-record-key-value
                 :schema pet-schema
                 :serialized-value (au/<? (<ser-str "Felis catus"))}

                ;; Make the array of pet records
                {:add-id "a5"
                 :item-id pets-array-item-id
                 :op-type :add-array-node
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-str pet1-item-id))}
                {:add-id "a6"
                 :item-id pets-array-item-id
                 :op-type :add-array-node
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-str pet2-item-id))}
                {:add-id "a7"
                 :item-id pets-array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge crdts/array-start-add-id
                                                     "a5"))}
                {:add-id "a8"
                 :item-id pets-array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge "a5" "a6"))}
                {:add-id "a8.5"
                 :item-id pets-array-item-id
                 :op-type :add-array-edge
                 :schema (l/array-schema pet-schema)
                 :serialized-value (au/<? (<ser-edge "a6"
                                                     crdts/array-end-add-id))}
                ;; Make the pet owner record
                {:add-id "a9"
                 :item-id alice-item-id
                 :k :name
                 :op-type :add-record-key-value
                 :schema pet-owner-schema
                 :serialized-value (au/<? (<ser-str "Alice"))}
                {:add-id "a10"
                 :item-id alice-item-id
                 :k :pets
                 :op-type :add-record-key-value
                 :schema pet-owner-schema
                 :serialized-value (au/<? (<ser-str pets-array-item-id))
                 :union-branch 1}

                ;; Make the id->pet-owner map
                {:add-id "a11"
                 :item-id map-item-id
                 :k "alice-id"
                 :op-type :add-map-key-value
                 :schema id-to-pet-owner-schema
                 :serialized-value (au/<? (<ser-str alice-item-id))}
                {:add-id "a12"
                 :item-id map-item-id
                 :k "alice-id"
                 :op-type :add-map-key
                 :schema id-to-pet-owner-schema}]
           annotated-ops (map #(assoc %
                                      :sys-time-ms (u/str->long "1640205282858"))
                              ops)
           _ (is (= true (au/<? (crdts/<apply-ops!
                                 {:ops annotated-ops
                                  :storage storage}))))
           crdt-info {:item-id map-item-id
                      :schema id-to-pet-owner-schema
                      :storage storage}
           expected {"alice-id" {:name "Alice",
                                 :pets [{:name "Bo"
                                         :species "Canis familiaris"}
                                        {:name "Sam"
                                         :species "Felis catus"}]}}
           v (au/<? (crdts/<get-crdt-val crdt-info))
           _ (is (= expected v))
           kv (au/<? (crdts/<get-crdt-val (assoc crdt-info :k "alice-id")))]
       (is (= (expected "alice-id") kv))))))

(deftest test-empty-stuff
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           item-id "my-str"
           schema l/string-schema
           ops []
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info (u/sym-map item-id schema storage)
           v (au/<? (crdts/<get-crdt-val crdt-info))
           _ (is (= nil v))]))))
