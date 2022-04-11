(ns integration.test-server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.oncurrent.zeno.authenticators.password.server :as password]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.server :as authz]
   [com.oncurrent.zeno.server :as server]
   [com.oncurrent.zeno.state-providers.crdt :as-alias crdt]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [integration.test-info :as ti]
   [integration.test-schemas :as test-schemas]
   [taoensso.timbre :as log]))

(defn get-tls-configs []
  (let [certificate-str (some-> (System/getenv "ZENO_SERVER_CERTIFICATE_FILE")
                                (slurp))
        private-key-str (some-> (System/getenv "ZENO_SERVER_PRIVATE_KEY_FILE")
                                (slurp))]
    (when-not certificate-str
      (throw (ex-info "Failed to load certificate file." {})))
    (when-not private-key-str
      (throw (ex-info "Failed to load private key file" {})))
    #:zeno{:certificate-str certificate-str
           :private-key-str private-key-str}))

(defn add-nums
  [{:zeno/keys [arg]}]
  (apply + arg))

(defn <get-name
  [{:zeno/keys [<get-state]}]
  (<get-state {:zeno/path [:zeno/crdt :name]}))

(defn <set-name!
  [{:zeno/keys [<set-state! arg]}]
  (au/go
    (au/<? (<set-state! {:zeno/path [:zeno/crdt :name]
                         :zeno/value arg}))
    true))

(defn <remove-name!
  [{:zeno/keys [<update-state!]}]
  (au/go
    (au/<? (<update-state! {:zeno/cmds [{:zeno/op :zeno/remove
                                         :zeno/path [:zeno/crdt :name]}]}))
    true))

(defn throw-if-even [{:zeno/keys [arg]}]
  (if (even? arg)
    (throw (ex-info "Even!" {}))
    false))

(defn -main [port-str tls?-str]
  (let [tls? (#{"true" "1"} (str/lower-case tls?-str))
        port (u/str->int port-str)
        crdt-sp (->state-provider #::crdt{:authorizer (authz/->authorizer)
                                          :schema test-schemas/crdt-schema})
        root->sp {:zeno/crdt crdt-sp}
        config #:zeno{:admin-password ti/admin-password
                      :authenticators [(password/make-authenticator)]
                      :port port
                      :root->state-provider root->sp
                      :rpcs test-schemas/rpcs
                      :storage (storage/make-storage)}
        _ (log/info (str "Starting Zeno integration test server on port "
                         port "."))
        zs (server/zeno-server (cond-> config
                                 tls? (merge (get-tls-configs))))]
    (server/set-rpc-handler! zs :add-nums add-nums)
    (server/set-rpc-handler! zs :get-name <get-name)
    (server/set-rpc-handler! zs :remove-name <remove-name!)
    (server/set-rpc-handler! zs :set-name <set-name!)
    (server/set-rpc-handler! zs :throw-if-even throw-if-even)
    zs))

(comment
  (defonce *zs (atom nil))
  (defn reset []
    (when @*zs
      (server/stop! @*zs))
    (reset! *zs (-main "8080" "false"))
    :ready)
  (reset))
