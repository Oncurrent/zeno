(ns integration.log-sync-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.identifier-secret.client :as isa]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-identifier []
  (str "user-" (u/compact-random-uuid) "@email.com"))

(l/def-record-schema data-schema
  [:numbers (l/array-schema l/int-schema)])

(deftest ^:this test-log-sync
  (au/test-async
   15000
   (ca/go
     (let [config {:branch "integration-test"
                   :crdt-schema data-schema
                   :get-server-url (constantly "ws://localhost:8080/client")}
           zc1 (zc/zeno-client (assoc config :client-name "zc1"))
           zc2 (zc/zeno-client (assoc config :client-name "zc2"))]
       (try
         (let [ch (ca/chan)
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
           (zc/shutdown! zc1)
           (zc/shutdown! zc2)))))))
