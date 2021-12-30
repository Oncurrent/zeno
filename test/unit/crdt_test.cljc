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

(l/def-record-schema major-minor-schema
  [:major l/int-schema]
  [:minor l/int-schema])

(l/def-record-schema example-rec-schema
  [:a l/float-schema]
  [:b l/int-schema]
  [:c major-minor-schema])

(def map-of-recs-schema (l/map-schema example-rec-schema))
(def map-of-ints-schema (l/map-schema l/int-schema))

;; TODO: Tests to add:
;; - Nested value arg

(defn <resolve-conflict-throw [{:keys [crdt-key current-value-infos]}]
  (au/go
    (throw
     (ex-info "Unexpected conflict" (u/sym-map crdt-key current-value-infos)))))

(deftest test-map-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser-int #(storage/<value->serialized-value storage l/int-schema %)
           item-id "my-map"
           schema (l/map-schema l/int-schema)
           subject-id "sid1234"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id subject-id sys-time-ms)
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
           subject-id "sid1234"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map item-id schema subject-id sys-time-ms)
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
                 :subject-id "sid1234"
                 :sys-time-ms (u/str->long "1640205282858")}
                {:add-id "a2"
                 :k :foo/a
                 :op-type :add-record-key-value
                 :item-id item-id
                 :schema the-rec-schema
                 :serialized-value (au/<? (<ser l/int-schema 42))
                 :subject-id "sid999"
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
           subject-id "sid1234"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map schema subject-id sys-time-ms)
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
           <ser-node #(storage/<value->serialized-value
                       storage l/string-schema %1)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           base-op (u/sym-map item-id schema)
           ops [;; Start state: ABC
                (assoc base-op
                       :add-id "a1"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "A")))
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "B")))
                (assoc base-op
                       :add-id "a3"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "C")))
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
                       :serialized-value (au/<? (<ser-node "X")))
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
                       :serialized-value (au/<? (<ser-node "Y")))
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
           <ser-node #(storage/<value->serialized-value
                       storage l/string-schema %1)
           <ser-edge (fn [head-node-add-id tail-node-add-id]
                       (storage/<value->serialized-value
                        storage
                        schemas/array-edge-schema
                        (u/sym-map head-node-add-id tail-node-add-id)))
           base-op (u/sym-map item-id schema)
           ops [;; Start state: ABC
                (assoc base-op
                       :add-id "a1"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "A")))
                (assoc base-op
                       :add-id "a2"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "B")))
                (assoc base-op
                       :add-id "a3"
                       :op-type :add-array-node
                       :serialized-value (au/<? (<ser-node "C")))
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
