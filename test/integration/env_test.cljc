(ns integration.env-test
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [com.oncurrent.zeno.authenticators.password.client :as pass]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [integration.test-info :as ti]
   #?(:clj kaocha.repl)
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-zc [{:keys [env]}]
  (let [config #:zeno{:env env
                      :get-server-url (constantly "ws://localhost:8080/client")}]
    (zc/zeno-client config)))

(def ex #?(:clj Exception :cljs js/Error))

(defn catcher [e]
  (log/error (u/ex-msg-and-stacktrace e))
  (is (= :threw :but-should-not-have)))

(deftest test-envs
  (au/test-async
   10000
   (au/go
     (let [admin (zc/admin-client
                  {:admin-password ti/admin-password
                   :get-server-url (constantly "ws://localhost:8080/admin")})]
       (zc/stop! admin)))))
