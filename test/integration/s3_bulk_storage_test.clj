(ns integration.s3-bulk-storage-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.bulk-storage :as bs]
   [com.oncurrent.zeno.utils :as u]
   [kaocha.repl]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(comment (kaocha.repl/run *ns*))

(deftest test-s3-bulk-storage-basic-ops
  (au/test-async
   3000
   (au/go
     (let [bucket-name (u/compact-random-uuid)
           _ (is (= true (au/<? (bs/<create-s3-bucket!
                                 (u/sym-map bucket-name)))))
           bulk-storage (bs/make-s3-bulk-storage (u/sym-map bucket-name))
           k "test-k"
           ba (ba/byte-array [1 2 3 42])
           _ (is (= nil (au/<? (bs/<get bulk-storage k))))
           _ (is (= true (au/<? (bs/<put! bulk-storage k ba))))
           ba-ret (au/<? (bs/<get bulk-storage k))
           _ (is (= true (ba/equivalent-byte-arrays? ba ba-ret)))
           _ (is (= true (au/<? (bs/<delete! bulk-storage k))))
           _ (is (= nil (au/<? (bs/<get bulk-storage k))))
           _ (is (= true (au/<? (bs/<delete-s3-bucket!
                                 (u/sym-map bucket-name)))))]))))
