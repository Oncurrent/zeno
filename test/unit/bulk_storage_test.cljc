(ns unit.bulk-storage-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   #?(:clj [com.oncurrent.zeno.bulk-storage :as bs])
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns*))

#?(:clj
   (deftest test-mem-bulk-storage-basic-ops
     (au/test-async
      1000
      (au/go
        (let [bulk-storage (bs/->mem-bulk-storage)
              k "test-k"
              ba (ba/byte-array [1 2 3 42])
              _ (is (= nil (au/<? (bs/<get bulk-storage k))))
              _ (is (= true (au/<? (bs/<put! bulk-storage k ba))))
              ba-ret (au/<? (bs/<get bulk-storage k))
              _ (is (= true (ba/equivalent-byte-arrays? ba ba-ret)))
              _ (is (= true (au/<? (bs/<delete! bulk-storage k))))
              _ (is (= nil (au/<? (bs/<get bulk-storage k))))])))))
