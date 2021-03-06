(ns com.oncurrent.zeno.schemas
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; Keeping schemas in a single cljc namespace simplifies Avro namespace mgmt

(def authenticator-name-schema l/keyword-schema)
(def branch-schema l/string-schema)
(def chunk-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def cluster-member-id-schema l/string-schema)
(def env-name-schema l/string-schema)
(def env-lifetime-mins-schema l/long-schema)
(def fingerprint-schema l/bytes-schema)
(def login-session-token-schema l/string-schema)
(def state-provider-name-schema l/keyword-schema)
(def timestamp-ms-schema l/long-schema)
(def ws-url-schema l/string-schema)

(l/def-enum-schema unauthenticated-actor-id-schema
  :unauthenticated)

(l/def-union-schema actor-id-schema
  l/string-schema
  unauthenticated-actor-id-schema)

(l/def-record-schema serialized-value-schema
  [:bytes l/bytes-schema]
  ;; TODO: Move this to another schema?
  [:chunk-ids "Used for chunked storage" (l/array-schema chunk-id-schema)]
  [:fp fingerprint-schema])

(l/def-record-schema chunk-schema
  [:bytes l/bytes-schema]
  [:chunk-i l/int-schema])

;;;;;;;;;;;;;;;; Server Schemas ;;;;;;;;;;;;;;;;;;;;;;

(def cluster-membership-list-schema
  (l/array-schema cluster-member-id-schema))

(l/def-record-schema mutex-info-schema
  [:lease-id l/string-schema]
  [:lease-length-ms l/int-schema]
  [:owner l/string-schema])

(l/def-record-schema cluster-member-info-schema
  [:ws-url ws-url-schema])

(l/def-enum-schema command-op-schema
  :zeno/insert-after
  :zeno/insert-before
  :zeno/insert-range-after
  :zeno/insert-range-before
  :zeno/remove
  :zeno/set)

(l/def-union-schema path-item-schema
  l/keyword-schema
  l/string-schema
  l/long-schema)

(l/def-array-schema path-schema
  path-item-schema)

;;;;;;;;;;;;;;; Authentication ;;;;;;;;;;;;;;;;

(l/def-record-schema login-session-info-schema
  [:login-session-token login-session-token-schema]
  [:login-session-token-minutes-remaining l/int-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema login-session-token-info-schema
  [:login-session-token-expiration-time-ms timestamp-ms-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema log-in-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:serialized-login-info serialized-value-schema])

(l/def-record-schema log-in-ret-schema
  [:serialized-extra-info serialized-value-schema]
  [:login-session-info login-session-info-schema])

(l/def-record-schema update-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:serialized-update-info serialized-value-schema]
  [:update-type l/keyword-schema])

(l/def-record-schema read-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:serialized-read-info serialized-value-schema]
  [:read-type l/keyword-schema])

;;;;;;;;;;;;;;; Envs ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema stored-authenticator-info-schema
  [:authenticator-name authenticator-name-schema]
  [:authenticator-branch branch-schema]
  [:authenticator-branch-source branch-schema])

(l/def-record-schema stored-state-provider-info-schema
  [:path-root l/keyword-schema]
  [:state-provider-name state-provider-name-schema]
  [:state-provider-branch branch-schema]
  [:state-provider-branch-source branch-schema])

(l/def-record-schema stored-env-info-schema
  [:env-name env-name-schema]
  [:stored-authenticator-infos (l/array-schema
                                stored-authenticator-info-schema)]
  [:stored-state-provider-infos (l/array-schema
                                 stored-state-provider-info-schema)])

(def env-name-to-info-schema (l/map-schema stored-env-info-schema))

;;;;;;;;;;;;;;; RPCs ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema rpc-anomaly-schema
  :zeno/rpc-error
  :zeno/rpc-unauthorized)

(l/def-record-schema rpc-arg-schema
  [:arg serialized-value-schema]
  [:rpc-id l/string-schema]
  [:rpc-name-kw-name l/string-schema]
  [:rpc-name-kw-ns l/string-schema])

(def rpc-ret-schema
  (l/union-schema [rpc-anomaly-schema serialized-value-schema]))

;;;;;;;;;;;;;;; State Provider Schemas ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema state-provider-msg-arg-schema
  [:arg serialized-value-schema]
  [:msg-type-name l/string-schema]
  [:msg-type-ns l/string-schema]
  [:state-provider-name state-provider-name-schema])

(l/def-record-schema state-provider-rpc-arg-schema
  [:arg serialized-value-schema]
  [:rpc-id l/string-schema]
  [:rpc-name-kw-name l/string-schema]
  [:rpc-name-kw-ns l/string-schema]
  [:state-provider-name state-provider-name-schema])

;;;;;;;;;;;;;;; Talk2 Protocols ;;;;;;;;;;;;;;;;;;;;;

(def admin-client-server-protocol
  {:create-env {:arg-schema stored-env-info-schema
                :ret-schema l/boolean-schema}
   :delete-env {:arg-schema env-name-schema
                :ret-schema l/boolean-schema}
   :get-env-names {:arg-schema l/null-schema
                   :ret-schema (l/array-schema env-name-schema)}
   :log-in {:arg-schema l/string-schema
            :ret-schema l/boolean-schema}})

(def client-server-protocol
  {:get-schema-pcf-for-fingerprint {:arg-schema fingerprint-schema
                                    :ret-schema (l/maybe l/string-schema)}
   :log-in {:arg-schema log-in-arg-schema
            :ret-schema (l/maybe log-in-ret-schema)}
   :log-out {:arg-schema l/null-schema
             :ret-schema l/boolean-schema}
   :resume-login-session {:arg-schema login-session-token-schema
                          :ret-schema (l/maybe login-session-info-schema)}
   :rpc {:arg-schema rpc-arg-schema
         :ret-schema rpc-ret-schema}
   :state-provider-msg {:arg-schema state-provider-msg-arg-schema}
   :state-provider-rpc {:arg-schema state-provider-rpc-arg-schema
                        :ret-schema rpc-ret-schema}
   :update-authenticator-state {:arg-schema
                                update-authenticator-state-arg-schema
                                :ret-schema serialized-value-schema}
   :read-authenticator-state {:arg-schema read-authenticator-state-arg-schema
                             :ret-schema serialized-value-schema}})
