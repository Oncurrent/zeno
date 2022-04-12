(ns integration.authentication-test
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   #?(:clj kaocha.repl)
   [com.oncurrent.zeno.authenticators.password.client :as password]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(comment (kaocha.repl/run 'integration.authentication-test))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-zc [{:keys [env-name]}]
  (let [config #:zeno{:env-name env-name
                      :get-server-base-url (constantly "ws://localhost:8080")}]
    (zc/->zeno-client config)))


(def ex #?(:clj Exception :cljs js/Error))

(defn catcher [e]
  (log/error (u/ex-msg-and-stacktrace e))
  (is (= :threw :but-should-not-have)))

(deftest test-password-authenticator
  (au/test-async
   10000
   (au/go
     (let [env-name "password-authenticator-test-env"
           zc (make-zc (u/sym-map env-name))
           password "A long-ish password with spaces CAPS 13212 !!#$@|_.*<>"
           actor-id (u/compact-random-uuid)
           login-ret (au/<? (password/<log-in!
                             {:password password
                              :zeno/actor-id actor-id
                              :zeno/zeno-client zc}))
           _ (is (not= false login-ret))
           _ (is (= true (au/<? (password/<add-actor-and-password!
                                 {:password password
                                  :zeno/actor-id actor-id
                                  :zeno/zeno-client zc}))))
           _ (is (= true(au/<? (password/<log-out!
                                {:zeno/zeno-client zc}))))
           new-password "Robert`);DROP TABLE Students;--"
           _ (is (= true (au/<? (password/<set-password!
                                 {:old-password password
                                  :new-password new-password
                                  :zeno/actor-id actor-id
                                  :zeno/zeno-client zc}))))
           _ (is (= true (au/<? (password/<log-out!
                                 {:zeno/zeno-client zc}))))
           _ (is (= false (au/<? (password/<log-in!
                                  {:password password
                                   :zeno/actor-id actor-id
                                   :zeno/zeno-client zc}))))
           login-ret2 (au/<? (password/<log-in!
                              {:password new-password
                               :zeno/actor-id actor-id
                               :zeno/zeno-client zc}))
           {:keys [login-session-token]} login-ret2
           zc2 (make-zc (u/sym-map env-name))
           rs-ret (au/<? (password/<resume-login-session!
                          zc2 login-session-token))]
       (is (= login-session-token (:login-session-token rs-ret)))
       (is (= false (au/<? (password/<resume-login-session!
                            zc2 "an-invalid-token"))))
       (zc/stop! zc)
       (zc/stop! zc2)))))
