(ns integration.crdt-sync-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server


(defn make-zc [{:keys [client-name env-name source-env-name]}]
  (let [crdt-sp (crdt-client/->state-provider
                 #::crdt{:authorizer nil ; TODO: Fill this in
                         :schema ts/crdt-schema})
        config #:zeno{:client-name client-name
                      :env-name env-name
                      :get-server-base-url (constantly "ws://localhost:8080")
                      :root->state-provider {:zeno/crdt crdt-sp}
                      :source-env-name source-env-name}]
    (zc/->zeno-client config)))


(deftest test-sync
  (au/test-async
   15000
   (ca/go
     (let [zc1 (make-zc {:client-name "zc1"})
           zc2 (make-zc {:client-name "zc2"})]
       (try
         (let [identifier1 (make-identifier)
               secret "secret"
               created-actor-id1 (au/<? (isa/<create-actor!
                                         zc1 identifier1 secret))
               login-ret1 (au/<? (isa/<log-in! zc1 identifier1 secret))
               _ (is (= created-actor-id1 (:actor-id login-ret1)))
               identifier2 (make-identifier)
               created-actor-id2 (au/<? (isa/<create-actor!
                                         zc1 identifier2 secret))
               login-ret2 (au/<? (isa/<log-in! zc2 identifier2 secret))
               _ (is (= created-actor-id2 (:actor-id login-ret2)))
               ch (ca/chan)
               _ (is (= true (au/<? (zc/<set-state! zc1
                                                    [:zeno/crdt :numbers]
                                                    [21]))))
               sub-map '{last-num [:zeno/crdt :numbers -1]}
               update-fn #(ca/put! ch %)]
           (is (= {'last-num nil} (zc/subscribe-to-state!
                                   zc2 "test" sub-map update-fn)))
           (is (= true (au/<? (zc/<update-state!
                               zc1
                               [{:zeno/arg 42
                                 :zeno/op :zeno/insert-after
                                 :zeno/path [:zeno/crdt :numbers -1]}]))))
           (is (= {'last-num 21} (au/<? ch)))
           (is (= {'last-num 42} (au/<? ch))))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/stop! zc1)
           (zc/stop! zc2)))))))
