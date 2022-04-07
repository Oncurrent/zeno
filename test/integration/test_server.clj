(ns integration.test-server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.oncurrent.zeno.authenticators.magic-token.server :as mt-auth]
   [com.oncurrent.zeno.authenticators.password.server :as password]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.server :as authz]
   [com.oncurrent.zeno.server :as server]
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

(defrecord MagicTokenApplicationServer
  [mins-valid number-of-uses]
  mt-auth/IMagicTokenApplicationServer
  (get-extra-info-schema [this] l/string-schema)
  (get-params-schema [this] l/string-schema)
  (<handle-request-magic-token! [this arg]
    (au/go
     (spit (-> arg :token-info :extra-info)
           (prn-str (select-keys arg [:actor-id :token :token-info :params]))
           :append true)))
  (<handle-redeem-magic-token! [this arg]
    (au/go
     (spit (-> arg :token-info :extra-info)
           (prn-str (select-keys arg [:token :token-info]))
           :append true))))

(defn make-mtas
  ([] (make-mtas {}))
  ([{:keys [mins-valid number-of-uses]}]
   (->MagicTokenApplicationServer mins-valid number-of-uses)))

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

(defn -main [port-str tls?-str]
  (let [tls? (#{"true" "1"} (str/lower-case tls?-str))
        password-auth (password/make-authenticator)
        magic-token-auth (mt-auth/make-authenticator {:mtas (make-mtas)})
        authenticators [password-auth magic-token-auth]
        port (u/str->int port-str)
        config #:zeno{:admin-password ti/admin-password
                      :authenticators authenticators
                      :port port
                      :rpcs test-schemas/rpcs
                      :storage (storage/make-storage)}
        _ (log/info (str "Starting Zeno integration test server on port "
                         port "."))
        zs (server/zeno-server (cond-> config
                                 tls? (merge (get-tls-configs))))]
    (server/set-rpc-handler! zs :add-nums add-nums)
    (server/set-rpc-handler! zs :get-name <get-name)
    (server/set-rpc-handler! zs :set-name <set-name!)
    (server/set-rpc-handler! zs :remove-name <remove-name!)
    zs))

(comment
  (defonce *zs (atom nil))
  (defn reset []
    (when @*zs
      (server/stop! @*zs))
    (reset! *zs (-main "8080" "false"))
    :ready)
  (reset))
