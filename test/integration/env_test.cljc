(ns integration.env-test
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [com.oncurrent.zeno.admin-client :as admin]
   [com.oncurrent.zeno.authenticators.password :as-alias pwd-auth]
   [com.oncurrent.zeno.authenticators.password.client :as pwd-client]
   [com.oncurrent.zeno.authenticators.password.shared :as pwd-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.state-providers.crdt :as-alias crdt]
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

(defn make-zc [{:keys [env-name]}]
  (let [crdt-sp (crdt-client/->state-provider
                 #::crdt{:authorizer nil ; TODO: Fill this in
                         :schema ts/crdt-schema})
        config #:zeno{:env-name env-name
                      :root->state-provider {:zeno/crdt crdt-sp}
                      :get-server-base-url (constantly "ws://localhost:8080")}]
    (zc/->zeno-client config)))

(deftest ^:this test-envs
  (au/test-async
   10000
   (au/go
     (let [admin (admin/admin-client
                  #:zeno{:admin-password ti/admin-password
                         :get-server-base-url
                         (constantly "ws://localhost:8080/admin")})
           ;; Create a permanent env to use as a base
           perm-env-name "test-env-perm"
           envs (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
           _ (when (= true (some #(= perm-env-name %) envs))
               (au/<? (admin/<delete-env!
                       #:zeno{:admin-client admin
                              :env-name perm-env-name})))
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
           ;; Connect to the permanent env and set some state
           zc-perm (make-zc {:env-name perm-env-name})
           sub-map {'name [:zeno/crdt :name]}
           get-state #(zc/subscribe-to-state! % "test" sub-map (constantly nil))
           _ (is (= '{name nil} (get-state zc-perm)))
           r1 (au/<? (zc/<update-state! zc-perm
                                        [{:zeno/arg "base"
                                          :zeno/op :zeno/set
                                          :zeno/path [:zeno/crdt :name]}]))
           _ (is (= '{name "base"} (get-state zc-perm)))
           ;; Create a temporary env based on the permanent env
           temp-env-name (u/compact-random-uuid)
           _ (is (= true (au/<? (admin/<create-temporary-env!
                                 #:zeno{:admin-client admin
                                        :env-name temp-env-name
                                        :lifetime-mins 5
                                        :source-env-name perm-env-name}))))
           ;; Connect to the temp env and set some state
           zc-temp (make-zc {:env-name temp-env-name})
           _ (is (= '{name "base"} (get-state zc-temp)))
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
           _ (log/info "AAAAAA")
           _ (is (= true aaap-ret))
           _ (log/info "BBBBB")
           ;; Log in as the new actor
           login-ret (au/<? (pwd-client/<log-in!
                             {:zeno/actor-id actor-id
                              :zeno/zeno-client zc-temp
                              ::pwd-auth/password actor-pwd}))
           _ (is (= actor-id (:actor-id login-ret)))
           _ (log/info "CCCCC")
           ;; Try to log in on the perm env as the new actor - Should fail
           _ (is (= false (au/<? (pwd-client/<log-in!
                                  {:zeno/actor-id actor-id
                                   :zeno/zeno-client zc-perm
                                   ::pwd-auth/password actor-pwd}))))
           _ (log/info "DDDDD")
           ;; Remove the perm env
           #_ (is (= true (au/<? (admin/<delete-env!
                                  #:zeno{:admin-client admin
                                         :env-name perm-env-name}))))]
       (admin/stop! admin)
       (zc/stop! zc-perm)
       (zc/stop! zc-temp)))))
