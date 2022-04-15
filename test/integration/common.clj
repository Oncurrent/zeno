(ns integration.common
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [integration.test-info :as ti]
   [integration.test-schemas :as ts]
   [com.oncurrent.zeno.admin-client :as admin]
   [com.oncurrent.zeno.authenticators.password.shared :as pwd-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.client :as crdt-client]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def rpcs
  {:add-nums {:arg-schema (l/array-schema l/int-schema)
              :ret-schema l/int-schema}})

(defn ->zc [{:keys [env-name source-env-name]}]
  (let [crdt-sp (crdt-client/->state-provider
                 #::crdt{:authorizer nil ; TODO: Fill this in
                         :schema ts/crdt-schema})
        config #:zeno{:env-name env-name
                      :get-server-base-url (constantly "ws://localhost:8080")
                      :root->state-provider {:zeno/crdt crdt-sp}
                      :source-env-name source-env-name}]
    (zc/->zeno-client config)))

(defn <setup-env! [{:keys [env-name]}]
  (au/go
    (let [admin (admin/->admin-client
                 #:zeno{:admin-password ti/admin-password
                        :get-server-base-url
                        (constantly "ws://localhost:8080/admin")})
          envs (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
          _ (when (= true (some #(= env-name %) envs))
              (au/<? (admin/<delete-env!
                      #:zeno{:admin-client admin
                             :env-name env-name})))
          auth-infos [#:zeno{:authenticator-name pwd-shared/authenticator-name}]]
      (au/<? (admin/<create-env!
              #:zeno{:admin-client admin
                     :authenticator-infos auth-infos
                     :env-name env-name})))))
