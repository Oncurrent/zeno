(ns unit.client-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.client :as zc]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(deftest test-subscribe-local-update
  (au/test-async
   3000
   (ca/go
     (let [zc (zc/zeno-client)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{page [:client :page]}
           expected '{page :home}]
       (is (= true
              (au/<? (zc/<update-state! zc [{:path [:client :page]
                                             :op :set
                                             :arg :home}]))))
       (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                               update-fn)))
       (zc/shutdown! zc)))))
