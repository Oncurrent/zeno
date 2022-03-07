(ns unit.storage-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-writing-and-reading-items
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           v1 [2 3 45]
           m {"a" 47
              "b" 22}]
       (is (= nil (au/<? (storage/<get storage
                                       "v-key"
                                       (l/array-schema l/int-schema)))))
       (is (= nil (au/<? (storage/<get storage
                                       "m-key"
                                       (l/array-schema l/int-schema)))))
       (is (= true (au/<? (storage/<add! storage
                                         "v-key"
                                         (l/array-schema l/int-schema)
                                         v1))))
       (is (= true (au/<? (storage/<add! storage
                                         "m-key"
                                         (l/map-schema l/int-schema)
                                         m))))
       (is (= v1 (au/<? (storage/<get storage
                                      "v-key"
                                      (l/array-schema l/int-schema)))))
       (is (= m (au/<? (storage/<get storage
                                     "m-key"
                                     (l/map-schema l/int-schema)))))))))

(deftest test-chunked-writing-and-reading-items
  (au/test-async
   1000
   (ca/go
     (let [raw-storage (storage/make-mem-raw-storage 10)
           storage (storage/make-storage raw-storage)
           v1 [2 3 45 2333 2 7777 6234333]]
       (is (= nil (au/<? (storage/<get storage
                                       "v-key"
                                       (l/array-schema l/int-schema)))))
       (is (= true (au/<? (storage/<add! storage
                                         "v-key"
                                         (l/array-schema l/int-schema)
                                         v1))))
       (is (= v1 (au/<? (storage/<get storage
                                      "v-key"
                                      (l/array-schema l/int-schema)))))
       ;; There should 4 keys: "v-key", "FP-TO-SCH...", and two data chunks
       (is (= 4 (-> (:*data raw-storage)
                    (deref)
                    (count))))))))

(deftest test-swap
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           v-k "_v-key"
           v1 [2 3 45]
           i-k "_i-key"]
       (is (= nil (au/<? (storage/<get storage
                                       v-k
                                       (l/array-schema l/int-schema)))))
       (is (= v1 (au/<? (storage/<swap!
                         storage
                         v-k
                         (l/array-schema l/int-schema)
                         (constantly v1)))))
       (is (= v1 (au/<? (storage/<get storage
                                      v-k
                                      (l/array-schema l/int-schema)))))
       (is (= nil (au/<? (storage/<get storage
                                       i-k
                                       l/int-schema))))
       (is (= 0 (au/<? (storage/<swap!
                        storage
                        i-k
                        l/int-schema
                        (constantly 0)))))
       (is (= 1 (au/<? (storage/<swap!
                        storage
                        i-k
                        l/int-schema
                        inc))))
       (is (= 1 (au/<? (storage/<get storage
                                     i-k
                                     l/int-schema))))))))

(deftest test-chunked-swap
  (au/test-async
   1000
   (ca/go
     (let [raw-storage (storage/make-mem-raw-storage 10)
           storage (storage/make-storage raw-storage)
           v-k "_v-key"
           v1 [1700 810000 333444555 42 78 999999999]]
       (is (= nil (au/<? (storage/<get storage
                                       v-k
                                       (l/array-schema l/int-schema)))))
       (is (= v1 (au/<? (storage/<swap!
                         storage
                         v-k
                         (l/array-schema l/int-schema)
                         (constantly v1)))))
       (is (= v1 (au/<? (storage/<get storage
                                      v-k
                                      (l/array-schema l/int-schema)))))
       ;; There should 4 keys: "_v-key", "FP-TO-SCH...", and two data chunks
       (is (= 4 (-> (:*data raw-storage)
                    (deref)
                    (count))))))))
