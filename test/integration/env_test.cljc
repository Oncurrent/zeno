(ns integration.env-test
  (:require
   [clojure.core.async :as ca]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [com.oncurrent.zeno.admin-client :as admin]
   [com.oncurrent.zeno.authenticators.password :as pwd-auth]
   [com.oncurrent.zeno.authenticators.password.client :as pwd-client]
   [com.oncurrent.zeno.authenticators.password.shared :as pwd-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt.shared :as crdt-shared]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   #?(:clj kaocha.repl)
   [taoensso.timbre :as log]
   [test-common :as c]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(comment (kaocha.repl/run *ns*))

;; TODO: Test creating a perm env based on an existing env
(comment
  (kaocha.repl/run #'test-envs {:color? false :capture-output? false}))
(deftest test-envs
  (au/test-async
   10000
   (au/go
     (au/<? (c/<clear-envs!))
     (let [admin (c/->admin)
           ;; Create a permanent env to use as a base
           perm-env-name (u/compact-random-uuid)
           auth-infos [#:zeno{:authenticator-name
                              pwd-shared/authenticator-name
                              ;; If :authenticator-branch is nil, Zeno will use
                              ;; the :env-name as the branch
                              :authenticator-branch nil}]
           spis [#:zeno{:path-root :zeno/crdt
                        :state-provider-name crdt-shared/state-provider-name
                        ;; If :state-provider-branch is nil, Zeno will use
                        ;; the :env-name as the branch
                        :state-provider-branch nil}]
           _ (is (= true (au/<? (admin/<create-env!
                                 #:zeno{:admin-client admin
                                        :authenticator-infos auth-infos
                                        :env-name perm-env-name
                                        :state-provider-infos spis}))))
           envs (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
           _ (is (= [perm-env-name] envs))
           ;; Connect to the permanent env and set some state
           zc-perm (c/->zc {:env-name perm-env-name})
           sub-map {'name [:zeno/crdt :name]}
           get-state #(zc/subscribe-to-state! % "test" sub-map (constantly nil))
           _ (is (= '{name nil} (get-state zc-perm)))
           r1 (au/<? (zc/<update-state! zc-perm
                                        [{:zeno/arg "base"
                                          :zeno/op :zeno/set
                                          :zeno/path [:zeno/crdt :name]}]))
           _ (is (= '{name "base"} (get-state zc-perm)))
           <wait-for-sync (-> zc-perm :root->state-provider :zeno/crdt
                              ::sp-impl/<wait-for-sync)
           _ (au/<? (<wait-for-sync))
           ;; Connect to a temp env based on the permanent env
           zc-temp (c/->zc {:source-env-name perm-env-name})
           temp-update-ch (ca/chan (ca/dropping-buffer 1))
           init-temp-state (zc/subscribe-to-state! zc-temp "temp" sub-map
                                                   #(ca/put! temp-update-ch %))
           ;; Initially, the name is nil because no state has been synced
           ;; to the `zc-temp` client
           _ (is (= '{name nil} init-temp-state))
           ;; Wait for the data from the server to sync and set the name
           ;; to "base"
           _ (is (= {'name "base"} (ca/<! temp-update-ch)))
           ;; Set the `:name` to "temp" in this env
           r1 (au/<? (zc/<update-state! zc-temp
                                        [{:zeno/arg "temp"
                                          :zeno/op :zeno/set
                                          :zeno/path [:zeno/crdt :name]}]))
           _ (is (= '{name "temp"} (get-state zc-temp)))
           ;; Verify that the perm env is unaffected
           _ (is (= '{name "base"} (get-state zc-perm)))
           ;; Add an actor to the temp env
           actor-id "actor1"
           actor-pwd "adslfkjfads1541l2hnjmlas 1q2k34213lk,sASrwqfsad  dasf"
           aaap-ret (au/<? (pwd-client/<add-actor-and-password!
                            {:zeno/actor-id actor-id
                             :zeno/zeno-client zc-temp
                             ::pwd-auth/password actor-pwd}))
           _ (is (= true aaap-ret))
           ;; Log in as the new actor
           login-ret (au/<? (pwd-client/<log-in!
                             {:zeno/actor-id actor-id
                              :zeno/zeno-client zc-temp
                              ::pwd-auth/password actor-pwd}))]
       (is (= actor-id (:actor-id login-ret)))
       ;; Try to log in on the perm env as the new actor - Should fail
       (is (= false (au/<? (pwd-client/<log-in!
                            {:zeno/actor-id actor-id
                             :zeno/zeno-client zc-perm
                             ::pwd-auth/password actor-pwd}))))
       ;; Remove the perm env
       (is (= true (au/<? (admin/<delete-env!
                           #:zeno{:admin-client admin
                                  :env-name perm-env-name}))))
       (admin/stop! admin)
       (zc/stop! zc-perm)
       (zc/stop! zc-temp)))))

(comment
  (kaocha.repl/run #'test-envs-authenticator-copy))
(deftest test-envs-authenticator-copy
  (au/test-async
   10000
   (au/go
     (au/<? (c/<clear-envs!))
     (let [base (u/compact-random-uuid)
           perm (u/compact-random-uuid)
           [actor1 password1] ["actor1" "password1"]
           [actor2 password2] ["actor2" "password2"]
           [actor3 password3] ["actor3" "password3"]
           [actor4 password4] ["actor4" "password4"]
           [actor5 password5] ["actor5" "password5"]
           [actor6 password6] ["actor6" "password6"]
           <add-actor-password! (fn [a p zc]
                                  (au/go
                                    (is (= true
                                           (au/<?
                                            (pwd-client/<add-actor-and-password!
                                             {:zeno/actor-id a
                                              ::pwd-auth/password p
                                              :zeno/zeno-client zc}))))))
           <test-log-in-out! (fn [a p zc should-work?]
                               (au/go
                                 (is (= should-work?
                                        (boolean
                                         (au/<? (pwd-client/<log-in!
                                                 {:zeno/actor-id a
                                                  ::pwd-auth/password p
                                                  :zeno/zeno-client zc})))))
                                 (is (= true (au/<? (pwd-client/<log-out!
                                                     {:zeno/zeno-client zc}))))))
           _ (is (= true (au/<? (c/<setup-env! {:env-name base}))))
           zc-base (c/->zc {:env-name base})
           _ (au/<? (<add-actor-password! actor1 password1 zc-base))
           _ (is (= true (au/<? (c/<setup-env!
                                 {:env-name perm
                                  :authenticator-branch-source base}))))
           zc-perm (c/->zc {:env-name perm})
           _ (au/<? (<add-actor-password! actor2 password2 zc-perm))
           zc-temp (c/->zc {:source-env-name perm})
           _ (au/<? (<add-actor-password! actor3 password3 zc-temp))]

       (au/<? (<test-log-in-out! actor1 password1 zc-base true))
       ;; This one fails
       (au/<? (<test-log-in-out! actor1 password1 zc-perm true))
       ;; This one fails
       (au/<? (<test-log-in-out! actor1 password1 zc-temp true))

       (au/<? (<test-log-in-out! actor2 password2 zc-base false))
       (au/<? (<test-log-in-out! actor2 password2 zc-perm true))
       (au/<? (<test-log-in-out! actor2 password2 zc-temp true))

       (au/<? (<test-log-in-out! actor3 password3 zc-base false))
       (au/<? (<test-log-in-out! actor3 password3 zc-perm false))
       (au/<? (<test-log-in-out! actor3 password3 zc-temp true))

       (au/<? (<add-actor-password! actor4 password4 zc-base))
       (au/<? (<add-actor-password! actor5 password5 zc-perm))
       (au/<? (<add-actor-password! actor6 password6 zc-temp))

       (au/<? (<test-log-in-out! actor4 password4 zc-base true))
       (au/<? (<test-log-in-out! actor4 password4 zc-perm false))
       (au/<? (<test-log-in-out! actor4 password4 zc-temp false))

       (au/<? (<test-log-in-out! actor5 password5 zc-base false))
       (au/<? (<test-log-in-out! actor5 password5 zc-perm true))
       (au/<? (<test-log-in-out! actor5 password5 zc-temp false))

       (au/<? (<test-log-in-out! actor6 password6 zc-base false))
       (au/<? (<test-log-in-out! actor6 password6 zc-perm false))
       (au/<? (<test-log-in-out! actor6 password6 zc-temp true))

       (zc/stop! zc-base)
       (zc/stop! zc-perm)
       (zc/stop! zc-temp)))))
