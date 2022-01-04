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
     (let [zc (zc/zeno-client {:initial-client-state
                               '{page :home}})
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{page [:client :page]}]
       (is (= '{page nil} (zc/subscribe-to-state!
                           zc "test" sub-map update-fn)))
       (is (= true
              (au/<? (zc/<update-state! zc [{:path [:client :page]
                                             :op :set
                                             :arg :login}]))))
       (is (= '{page :login} (au/<? ch)))
       (zc/shutdown! zc)))))
#_
(deftest test-subscribe-sys-update
  (au/test-async
   3000
   (ca/go
     (let [sys-schema l/string-schema
           zc (zc/zeno-client (u/sym-map sys-schema))
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           sub-map '{the-string [:sys]}]
       (is (= '{the-string nil} (zc/subscribe-to-state!
                                 zc "test" sub-map update-fn)))
       (is (= true
              (au/<? (zc/<update-state! zc [{:path [:sys]
                                             :op :set
                                             :arg "Hi"}]))))
       #_(is (= '{the-string "Hi"} (au/<? ch)))
       (zc/shutdown! zc)))))
