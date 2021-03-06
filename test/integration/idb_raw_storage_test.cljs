(ns integration.idb-raw-storage-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.client.idb-raw-storage :as irs]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(deftest test-read-write-delete
  (au/test-async
   2000
   (ca/go
     (let [raw-storage (irs/make-idb-raw-storage "test" "store")
           k1 "key-1"
           ba1 (ba/byte-array [1 2 3 42])
           _ (is (= true (au/<? (storage/<delete-k! raw-storage k1))))
           _ (is (= nil (au/<? (storage/<read-k raw-storage k1))))
           _ (is (= true (au/<? (storage/<add-k! raw-storage k1 ba1))))
           ret (au/<? (storage/<read-k raw-storage k1))]
       (is (ba/equivalent-byte-arrays? ba1 ret))
       (is (thrown-with-msg?
            js/Error
            #"exists"
            (au/<? (storage/<add-k! raw-storage k1 ba1))))
       (is (= true (au/<? (storage/<delete-k! raw-storage k1))))
       (is (= nil (au/<? (storage/<read-k raw-storage k1))))))))

(deftest test-compare-and-set
  (au/test-async
   2000
   (ca/go
     (let [raw-storage (irs/make-idb-raw-storage "test" "store")
           k "xyz"
           v1 (ba/byte-array [2 3 45])
           v2 (ba/byte-array [7 123 8])
           v3 (ba/byte-array [8 7 10 32])]
       (is (= true (au/<? (storage/<delete-k! raw-storage k))))
       (is (= true (au/<? (storage/<compare-and-set-k! raw-storage k nil v1))))
       (is (= true (au/<? (storage/<compare-and-set-k! raw-storage k v1 v2))))
       (is (= false (au/<? (storage/<compare-and-set-k! raw-storage k v1 v3))))
       (is (= true (au/<? (storage/<compare-and-set-k! raw-storage k v2 v3))))
       (is (ba/equivalent-byte-arrays?
            v3
            (au/<? (storage/<read-k raw-storage k))))))))
