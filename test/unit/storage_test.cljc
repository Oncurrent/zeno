(ns unit.storage-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
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
           v2 [7 1234 8]
           m {"a" 47
              "b" 22}]
       (is (= nil (au/<? (storage/<get storage
                                       "v-key"
                                       (l/array-schema l/int-schema)))))
       (is (= true (au/<? (storage/<put! storage
                                         "v-key"
                                         (l/array-schema l/int-schema)
                                         v1))))
       (is (= true (au/<? (storage/<put! storage
                                         "m-key"
                                         (l/map-schema l/int-schema)
                                         m))))
       (is (= v1 (au/<? (storage/<get storage
                                      "v-key"
                                      (l/array-schema l/int-schema)))))
       (is (= m (au/<? (storage/<get storage
                                     "m-key"
                                     (l/map-schema l/int-schema)))))
       (is (= true (au/<? (storage/<put! storage
                                         "v-key"
                                         (l/array-schema l/int-schema)
                                         v2))))
       (is (= v2 (au/<? (storage/<get storage
                                      "v-key"
                                      (l/array-schema l/int-schema)))))
       (is (= m (au/<? (storage/<get storage
                                     "m-key"
                                     (l/map-schema l/int-schema)))))))))

(deftest test-compare-and-set
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           k "v-key"
           v1 [2 3 45]
           v2 [7 1234 8]
           v3 [8 777 1 988 8222]]
       (is (= true (au/<? (storage/<compare-and-set!
                           storage
                           k
                           (l/array-schema l/int-schema)
                           nil
                           v1))))
       (is (= true (au/<? (storage/<compare-and-set!
                           storage
                           k
                           (l/array-schema l/int-schema)
                           v1
                           v2))))
       (is (= false (au/<? (storage/<compare-and-set!
                            storage
                            k
                            (l/array-schema l/int-schema)
                            v1
                            v3))))
       (is (= true (au/<? (storage/<compare-and-set!
                           storage
                           k
                           (l/array-schema l/int-schema)
                           v2
                           v3))))
       (is (= v3 (au/<? (storage/<get storage
                                      k
                                      (l/array-schema l/int-schema)))))))))
