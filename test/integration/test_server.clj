(ns integration.test-server
  (:require
   [clojure.string :as str]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.identifier-secret.server :as is-auth]
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

(defn -main [port-str tls?-str]
  (let [tls? (#{"true" "1"} (str/lower-case tls?-str))
        branch "integration-test"
        identity-secret-auth (is-auth/make-authenticator)
        authenticators [identity-secret-auth]
        port (u/str->int port-str)
        config {:branch->authenticators {branch authenticators}
                :crdt-schema data-schema
                :port port
                :storage (storage/make-storage)}]
    (log/info (str "Starting Zeno integration test server on port " port "."))
    (server/zeno-server (cond-> config
                          tls? (merge (get-tls-configs))))))
