(ns unit.crdt-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdts :as crdts]
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

#_
(deftest test-cmd->crdt-ops
  (let [cmd {:arg 1
             :op :set
             :path ["a" :b]}
        storage (storage/make-storage)
        *unique-id-num (atom 0)
        get-unique-id #(str (swap! *unique-id-num inc))
        arg (u/sym-map cmd storage get-unique-id sys-schema)
        ret (crdts/cmd->crdt-ops arg)
        expected :foo]
    (is (= expected ret))))


;; TODO: Tests to add:
;; - Nested value arg
;; - Arrays

(defn <resolve-conflict-throw [{:keys [crdt-key current-value-infos]}]
  (au/go
    (throw
     (ex-info "Unexpected conflict" (u/sym-map crdt-key current-value-infos)))))

(deftest test-map-crdt-basic-ops
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           <ser #(storage/<value->serialized-value storage %1 %2)
           map-id "my-map"
           subject-id "sid1234"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map map-id subject-id sys-time-ms)
           ops [(assoc base-op
                       :add-id "a1"
                       :k "a"
                       :op-type :add-map-key-value
                       :serialized-value (au/<? (<ser l/int-schema 1)))
                (assoc base-op
                       :add-id "a2"
                       :k "a"
                       :op-type :add-map-key)
                (assoc base-op
                       :add-id "a3"
                       :k "b"
                       :op-type :add-map-key-value
                       :serialized-value (au/<? (<ser l/int-schema 2)))
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
                       :serialized-value (au/<? (<ser l/int-schema 42)))
                (assoc base-op
                       :add-id "a3"
                       :op-type :del-map-key-value)
                (assoc base-op
                       :add-id "a4"
                       :op-type :del-map-key)]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info {:<resolve-conflict <resolve-conflict-throw
                      :map-id map-id
                      :schema (l/map-schema l/int-schema)
                      :storage storage
                      :subject-id subject-id}
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
           record-id "for-the-record"
           subject-id "sid1234"
           sys-time-ms (u/str->long "1640205282858")
           base-op (u/sym-map record-id subject-id sys-time-ms)
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
           crdt-info {:<resolve-conflict <resolve-conflict-throw
                      :record-id record-id
                      :storage storage
                      :schema the-rec-schema}
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
           record-id "for-the-record"
           ops [{:add-id "a1"
                 :k :foo/a
                 :op-type :add-record-key-value
                 :record-id record-id
                 :serialized-value (au/<? (<ser l/int-schema 1))
                 :subject-id "sid1234"
                 :sys-time-ms (u/str->long "1640205282858")}
                {:add-id "a2"
                 :k :foo/a
                 :op-type :add-record-key-value
                 :record-id record-id
                 :serialized-value (au/<? (<ser l/int-schema 42))
                 :subject-id "sid999"
                 :sys-time-ms (u/str->long "1640276685205")}]
           _ (is (= true (au/<? (crdts/<apply-ops! (u/sym-map ops storage)))))
           crdt-info {:<resolve-conflict u/<resolve-conflict-lww
                      :make-add-id (constantly "a3")
                      :record-id record-id
                      :schema the-rec-schema
                      :storage storage
                      :subject-id "sid333"}
           v (au/<? (crdts/<get-crdt-val crdt-info))
           expected-v {:foo/a 42}
           _ (is (= expected-v v))]))))
