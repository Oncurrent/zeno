(ns integration.authentication-test
  (:require
   [clojure.core.async :as ca]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   #?(:clj kaocha.repl)
   [com.oncurrent.zeno.authenticators.password :as pwd-auth]
   [com.oncurrent.zeno.authenticators.password.client :as password]
   [com.oncurrent.zeno.authenticators.password.shared :as pwd-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]
   [test-common :as c]))

(comment (kaocha.repl/run *ns*))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(comment (kaocha.repl/run #'test-password-authenticator {:capture-output? false}))
(deftest test-password-authenticator
  (au/test-async
   10000
   (au/go
     (let [env-name "test-auth-env"
           _ (is (= true (au/<? (c/<setup-env! (u/sym-map env-name)))))
           zc (c/->zc (u/sym-map env-name))
           password "A long-ish password with spaces CAPS 13212 !!#$@|_.*<>"
           actor-id (u/compact-random-uuid)
           login-ret (au/<? (password/<log-in!
                             {::pwd-auth/password password
                              :zeno/actor-id actor-id
                              :zeno/zeno-client zc}))
           _ (is (= false login-ret))
           _ (is (= true (au/<? (password/<add-actor-and-password!
                                 {::pwd-auth/password password
                                  :zeno/actor-id actor-id
                                  :zeno/zeno-client zc}))))
           login-ret (au/<? (password/<log-in!
                             {::pwd-auth/password password
                              :zeno/actor-id actor-id
                              :zeno/zeno-client zc}))
           _ (is (not= false login-ret))
           _ (is (= true (au/<? (password/<log-out!
                                 {:zeno/zeno-client zc}))))
           new-password "Robert`);DROP TABLE Students;--"
           _ (is (= true (au/<? (password/<set-password!
                                 {::pwd-auth/old-password password
                                  ::pwd-auth/new-password new-password
                                  :zeno/actor-id actor-id
                                  :zeno/zeno-client zc}))))
           _ (is (= true (au/<? (password/<log-out!
                                 {:zeno/zeno-client zc}))))
           _ (is (= false (au/<? (password/<log-in!
                                  {::pwd-auth/password password
                                   :zeno/actor-id actor-id
                                   :zeno/zeno-client zc}))))
           login-ret2 (au/<? (password/<log-in!
                              {::pwd-auth/password new-password
                               :zeno/actor-id actor-id
                               :zeno/zeno-client zc}))
           {:keys [login-session-token]} login-ret2
           zc2 (c/->zc (u/sym-map env-name))
           rs-ret (au/<? (password/<resume-login-session!
                          {:zeno/login-session-token login-session-token
                           :zeno/zeno-client zc2}))
           ]
       (is (= login-session-token (:login-session-token rs-ret)))
       (is (= false (au/<? (password/<resume-login-session!
                            {:zeno/login-session-token "an-invalid-token"
                             :zeno/zeno-client zc2}))))
       (zc/stop! zc)
       (zc/stop! zc2)
       ))))
