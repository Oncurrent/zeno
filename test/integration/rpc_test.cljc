(ns integration.rpc-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [integration.test-schemas :as test-schemas]
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(deftest ^:this test-rpc
  (au/test-async
   10000
   (ca/go
     (let [config #:zeno{:crdt-schema test-schemas/crdt-schema
                         :get-server-base-url (constantly
                                               "ws://localhost:8080")
                         :rpcs test-schemas/rpcs}
           zc (zc/zeno-client config)]
       (try
         (let [arg [1 3 6]
               ret (au/<? (zc/<rpc! zc :add-nums arg))
               expected (apply + arg)
               the-name "Bonzo"]
           (is (= nil (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= true (au/<? (zc/<rpc! zc :set-name the-name))))
           (is (= the-name (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= true (au/<? (zc/<rpc! zc :remove-name nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= false (au/<? (zc/<rpc! zc :throw-if-even 41))))
           (try
             (au/<? (zc/<rpc! zc :throw-if-even 42))
             (is (= :should-have-thrown :but-didnt))
             (catch #?(:clj Exception :cljs js/Error) e
               (log/info "XXXX")
               (is (= :should-throw :should-throw)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/stop! zc)))))))
