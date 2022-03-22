(ns integration.test-server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.identifier-secret.server :as is-auth]
   [com.oncurrent.zeno.authenticators.magic-token.server :as mt-auth]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.server :as authz]
   [com.oncurrent.zeno.server :as server]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [integration.test-schemas :as test-schemas]
   [taoensso.timbre :as log]))

(l/def-record-schema data-schema
  [:numbers (l/array-schema l/int-schema)])

(defn get-tls-configs []
  (let [certificate-str (some-> (System/getenv "ZENO_SERVER_CERTIFICATE_FILE")
                                (slurp))
        private-key-str (some-> (System/getenv "ZENO_SERVER_PRIVATE_KEY_FILE")
                                (slurp))]
    (when-not certificate-str
      (throw (ex-info "Failed to load certificate file." {})))
    (when-not private-key-str
      (throw (ex-info "Failed to load private key file" {})))
    (u/sym-map certificate-str private-key-str)))

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

(defn <add-nums [{:keys [arg conn-id get-in-crdt]}]
  (au/go
    (let [v (get-in-crdt {:branch nil ; defaults to caller's branch
                          :path []})]
      (log/info (str "Adding nums: "
                     (u/pprint-str (u/sym-map arg conn-id v))))
      (apply + arg))))

(defn -main [port-str tls?-str]
  (let [tls? (#{"true" "1"} (str/lower-case tls?-str))
        branch "integration-test"
        identity-secret-auth (is-auth/make-authenticator)
        magic-token-auth (mt-auth/make-authenticator {:mtas (make-mtas)})
        authenticators [identity-secret-auth magic-token-auth]
        port (u/str->int port-str)
        config {:authenticators authenticators
                :crdt-authorizer (authz/make-affirmative-authorizer)
                :crdt-branch branch
                :crdt-schema data-schema
                :port port
                :rpcs test-schemas/rpcs
                :storage (storage/make-storage)}
        _ (log/info (str "Starting Zeno integration test server on port "
                         port "."))
        zs (server/zeno-server (cond-> config
                                 tls? (merge (get-tls-configs))))]
    (server/set-rpc-handler! zs :add-nums <add-nums)))

(comment
  (defonce *zs (atom nil))
  (defn reset []
    (when @*zs
      (server/stop! @*zs))
    (reset! *zs (-main "8080" "false"))
    :ready)
  (reset))
