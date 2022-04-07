(ns integration.env-test
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [com.oncurrent.zeno.admin-client :as admin]
   [com.oncurrent.zeno.authenticators.password.client :as password-client]
   [com.oncurrent.zeno.authenticators.password.shared :as password-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.state-providers.crdt.client :as crdt-client]
   [com.oncurrent.zeno.state-providers.crdt.shared :as crdt-shared]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [integration.test-info :as ti]
   [integration.test-schemas :as ts]
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

(deftest ^:this test-envs
  (au/test-async
   10000
   (au/go
     (let [admin (admin/admin-client
                  #:zeno{:admin-password ti/admin-password
                         :get-server-url (constantly "ws://localhost:8080/admin")})
           ;; Create a permanent env to use as a base
           perm-env-name "test-env-perm"
           envs (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
           _ (when (some #(= perm-env-name %) envs)
               (au/<? (admin/<delete-env! {:zeno/admin-client admin})))
           auth-infos [#:zeno{:authenticator-name
                              password-shared/authenticator-name
                              ;; If :authenticator-branch is nil, Zeno will use
                              ;; the :env-name as the branch, creating it if
                              ;; necessary
                              :authenticator-branch nil}]
           crdt-info #:zeno{:state-provider-name crdt-shared/state-provider-name
                            ;; If :state-provider-branch is nil, Zeno will use
                            ;; the :env-name as the branch, creating it if
                            ;; necessary
                            :state-provider-branch nil}
           root->spi {:zeno/crdt crdt-info}
           env-arg #:zeno{:admin-client admin
                          :authenticator-infos auth-infos
                          :env-name perm-env-name
                          :root->state-provider-info root->spi}
           _ (is (= true (au/<? (admin/<create-env! env-arg))))
           envs1 (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
           _ (is (= true (some #(= perm-env-name %) envs1)))
           ;; Connect to the permanent env and set some state
           zc-perm (make-zc {:env perm-env-name})
           sub-map {'numbers [:zeno/crdt :numbers]}
           get-state #(zc/subscribe-to-state! % "test" sub-map (constantly nil))
           _ (is (= '{numbers nil} (get-state zc-perm)))
           r1 (au/<? (zc/<update-state! zc-perm [{:zeno/arg [1 2 3]
                                                  :zeno/op :set
                                                  :zeno/path [:numbers]}]))
           _ (is (= '{numbers [1 2 3]} (get-state zc-perm)))
           ;; Create a temporary env based on the permanent env
           temp-env-name (u/compact-random-uuid)
           _ (is (= true (au/<? (admin/<create-temporary-env!
                                 #:zeno{:admin-client admin
                                        :env-name temp-env-name
                                        :lifetime-mins 5
                                        :source-env-name perm-env-name}))))
           ;; Connect to the temp env and set some state
           zc-temp (make-zc {:env perm-env-name})
           sub-map {'numbers [:zeno/crdt :numbers]}
           get-state #(zc/subscribe-to-state! % "test" sub-map (constantly nil))
           _ (is (= '{numbers nil} (get-state zc-temp)))
           r1 (au/<? (zc/<update-state! zc-temp [{:zeno/arg 42
                                                  :zeno/op :insert-after
                                                  :zeno/path [:numbers -1]}]))
           _ (is (= '{numbers [1 2 3 42]} (get-state zc-temp)))
           ;; Verify that the perm env is unaffected
           _ (is (= '{numbers [1 2 3]} (get-state zc-perm)))
           ;; Add an actor to the temp env
           actor-id "actor1"
           actor-pwd "adslfkjfads1541l2hnjmlas 1q2k34213lk,sASrwqfsad  dasf"
           _ (is (= true (au/<? (password-client/<add-actor-and-password!
                                 {:zeno/actor-id actor-id
                                  :zeno/zeno-client zc-temp
                                  :password actor-pwd}))))
           ;; Log in as the new actor
           _ (is (= true (au/<? (password-client/<log-in!
                                 {:zeno/actor-id actor-id
                                  :zeno/zeno-client zc-temp
                                  :password actor-pwd}))))
           ;; Try to log in on the perm env as the new actor - Should fail
           _ (is (= false (au/<? (password-client/<log-in!
                                  {:zeno/actor-id actor-id
                                   :zeno/zeno-client zc-perm
                                   :password actor-pwd}))))
           ;; Remove the perm env
           _ (is (= true (au/<? (admin/<delete-env!
                                 #:zeno{:admin-client admin
                                        :env-name perm-env-name}))))
           envs2 (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
           _ (is (= false (some #(= perm-env-name %) envs2)))]
       (admin/stop! admin)
       (zc/stop! zc-perm)
       (zc/stop! zc-temp)))))
