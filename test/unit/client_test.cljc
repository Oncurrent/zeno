(ns unit.client-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   #?(:clj [kaocha.repl])
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]
   [test-common :as c]))

(comment (kaocha.repl/run *ns*))

(deftest test-subscribe-client-update
  (au/test-async
   3000
   (ca/go
     (let [zc (c/->zc-unit {:initial-client-state '{page :home}})
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{page [:zeno/client :page]}]
       (is (= '{page nil} (zc/subscribe-to-state!
                           zc "test" sub-map update-fn)))
       (is (= true
              (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :page]
                                             :zeno/op :zeno/set
                                             :zeno/arg :login}]))))
       (is (= '{page :login} (au/<? ch)))
       (zc/stop! zc)))))
