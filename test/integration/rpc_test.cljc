(ns integration.rpc-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log]
   [test-common :as c]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server
(comment (kaocha.repl/run *ns*))

(comment
 (kaocha.repl/run #'test-rpc))
(deftest test-rpc
  (au/test-async
   10000
   (au/go
     (let [config #:zeno{:get-server-base-url (constantly
                                               "ws://localhost:8080")
                         :rpcs c/rpcs}
           zc (zc/->zeno-client config)]
       (try
         (let [arg [1 3 6]
               ret (au/<? (zc/<rpc! zc :add-nums arg))
               expected (apply + arg)
               the-name "Bonzo"]
           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= true (au/<? (zc/<rpc! zc :set-name the-name))))
           (is (= the-name (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= true (au/<? (zc/<rpc! zc :remove-name nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-name nil))))
           (is (= false (au/<? (zc/<rpc! zc :throw-if-even 41))))
           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-crdt nil))))
           (try
             (au/<? (zc/<rpc! zc :throw-if-even 42))
             (is (= :should-have-thrown :but-didnt))
             (catch #?(:clj Exception :cljs js/Error) e
               (is (= :should-throw :should-throw)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/stop! zc)))))))

(comment
 (kaocha.repl/run #'test-crdt-state {:color? false}))
(deftest test-crdt-state
  (au/test-async
   10000
   (au/go
     (let [config #:zeno{:get-server-base-url (constantly
                                               "ws://localhost:8080")
                         :rpcs c/rpcs}
           zc (zc/->zeno-client config)]
       (try
         (let [the-name {:name "Bonzo"}
               nested {:nested {:a {:aa 1}}}]
           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-crdt nil))))
           (is (= true (au/<? (zc/<rpc! zc :set-crdt the-name))))
           (is (= the-name (au/<? (zc/<rpc! zc :get-crdt nil))))
           (is (= true (au/<? (zc/<rpc! zc :remove-name nil))))
           (is (= {} (au/<? (zc/<rpc! zc :get-crdt nil))))

           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-crdt nil))))

           (is (= true (au/<? (zc/<rpc! zc :set-crdt nested))))
           (is (= nested (au/<? (zc/<rpc! zc :get-crdt nil))))

           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-crdt nil))))

           (is (= true (au/<? (zc/<rpc! zc :set-nested (:nested nested)))))
           (is (= nested (au/<? (zc/<rpc! zc :get-crdt nil))))

           (is (= true (au/<? (zc/<rpc! zc :set-crdt nil))))
           (is (= nil (au/<? (zc/<rpc! zc :get-crdt nil)))))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/stop! zc)))))))
