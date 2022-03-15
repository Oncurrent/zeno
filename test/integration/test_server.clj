(ns integration.test-server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.identifier-secret.server :as is-auth]
   [com.oncurrent.zeno.authenticators.magic-token.server :as mt-auth]
   [com.oncurrent.zeno.server :as server]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
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
  (<handle-request-magic-token! [this {:keys [token token-info]}]
    (au/go
      (spit (:extra-info token-info) (str token "\n") :append true)))
  (<handle-redeem-magic-token! [this {:keys [token-info]}]
    (au/go
      (spit (:extra-info token-info) "did-action!\n" :append true))))

(defn make-mtas
  ([] (make-mtas {}))
  ([{:keys [mins-valid number-of-uses]}]
   (->MagicTokenApplicationServer mins-valid number-of-uses)))

(defn -main [port-str tls?-str]
  (let [tls? (#{"true" "1"} (str/lower-case tls?-str))
        branch "integration-test"
        identity-secret-auth (is-auth/make-authenticator)
        magic-token-auth (mt-auth/make-authenticator {:mtas (make-mtas)})
        authenticators [identity-secret-auth magic-token-auth]
        port (u/str->int port-str)
        config {:branch->authenticators {branch authenticators}
                :crdt-schema data-schema
                :port port
                :storage (storage/make-storage)}]
    (log/info (str "Starting Zeno integration test server on port " port "."))
    (server/zeno-server (cond-> config
                          tls? (merge (get-tls-configs))))))

(comment
  (defonce *zs (atom nil))
  (defn reset []
    (when @*zs
      (server/stop! @*zs))
    (reset! *zs (-main "8080" "false"))
    :ready)
  (reset))
