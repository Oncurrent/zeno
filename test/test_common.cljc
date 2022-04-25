(ns test-common
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.admin-client :as admin]
   [com.oncurrent.zeno.authenticators.password.shared :as pwd-shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.client :as aa]
   [com.oncurrent.zeno.state-providers.client-mem :as cm]
   [com.oncurrent.zeno.state-providers.client-mem.client :as cm-client]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.shared :as crdt-shared]
   [com.oncurrent.zeno.state-providers.crdt.client :as crdt-client]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def ex #?(:clj Exception :cljs js/Error))

(defn catcher [e]
  (log/error (u/ex-msg-and-stacktrace e))
  (is (= :threw :but-should-not-have)))

(def admin-password (str "hYczFGGPRFkKGK3yQxDGz7imUbDXAE8iRtUs.tfvAYqsVbeTQN"
                         "jbYwHB_skGw!ft4yMDrMD@uxwe@9an-iqa2ZYr3cAtmGt2MW_i"))

(l/def-record-schema book-schema
  [:title l/string-schema]
  [:nums (l/array-schema l/int-schema)])

(l/def-record-schema msg-schema
  [:text l/string-schema]
  [:user-id l/string-schema])

(l/def-record-schema crdt-schema
  [:books (l/map-schema book-schema)]
  [:msgs (l/array-schema msg-schema)]
  [:my-book-ids (l/array-schema l/string-schema)]
  [:name l/string-schema]
  [:num l/int-schema]
  [:numbers (l/array-schema l/int-schema)]
  [:the-id l/string-schema])

(def rpcs
  {:add-nums {:arg-schema (l/array-schema l/int-schema)
              :ret-schema l/int-schema}
   :get-name {:arg-schema l/null-schema
              :ret-schema (l/maybe l/string-schema)}
   :remove-name {:arg-schema l/null-schema
                 :ret-schema l/boolean-schema}
   :set-name {:arg-schema l/string-schema
              :ret-schema l/boolean-schema}
   :throw-if-even {:arg-schema l/int-schema
                   :ret-schema l/boolean-schema}})

(defn ->zc-unit
  ([] (->zc-unit nil))
  ([{crdt-schema* :crdt-schema
     :keys [initial-client-state]}]
   (let [root->sp {:zeno/client (cm-client/->state-provider
                                 #::cm{:initial-state initial-client-state})
                   :zeno/crdt (crdt-client/->state-provider
                               #::crdt{:authorizer (aa/->authorizer)
                                       :schema (or crdt-schema*
                                                   crdt-schema)
                                       :root :zeno/crdt})}]
     (zc/->zeno-client
      #:zeno{:env-lifetime-mins 1
             :root->state-provider root->sp}))))

(defn ->zc
  ([] (->zc {:env-lifetime-mins 1}))
  ([{:keys [env-name source-env-name env-lifetime-mins]}]
   (let [client-mem-sp (cm-client/->state-provider)
         crdt-sp (crdt-client/->state-provider
                  #::crdt{:authorizer (aa/->authorizer)
                          :schema crdt-schema
                          :root :zeno/crdt})
         root->sp {:zeno/client client-mem-sp :zeno/crdt crdt-sp}
         config #:zeno{:env-lifetime-mins env-lifetime-mins
                       :env-name env-name
                       :get-server-base-url (constantly "ws://localhost:8080")
                       :root->state-provider root->sp
                       :source-env-name source-env-name}]
     (zc/->zeno-client config))))

(defn ->admin []
  (admin/->admin-client
   #:zeno{:admin-password admin-password
          :get-server-base-url
          (constantly "ws://localhost:8080/admin")}))

(defn <setup-env! [{:keys [env-name authenticator-branch-source]}]
  (au/go
   (let [admin (->admin)
         envs (au/<? (admin/<get-env-names {:zeno/admin-client admin}))
         _ (when (= true (some #(= env-name %) envs))
             (au/<? (admin/<delete-env!
                     #:zeno{:admin-client admin
                            :env-name env-name})))
         auth-infos [#:zeno{:authenticator-name pwd-shared/authenticator-name
                            :authenticator-branch-source
                            authenticator-branch-source}]
         sp-infos [#:zeno{:path-root :zeno/crdt
                          :state-provider-name crdt-shared/state-provider-name}]
         ret (au/<? (admin/<create-env!
                     #:zeno{:admin-client admin
                            :authenticator-infos auth-infos
                            :state-provider-infos sp-infos
                            :env-name env-name}))]
      (admin/stop! admin)
      ret)))

(defn <clear-envs!
  ([] (<clear-envs! (->admin)))
  ([admin]
   (au/go
    (doseq [env (au/<? (admin/<get-env-names {:zeno/admin-client admin}))]
      (au/<? (admin/<delete-env!
              #:zeno{:admin-client admin
                     :env-name env})))
    (admin/stop! admin))))
