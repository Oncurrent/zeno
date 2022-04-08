(ns com.oncurrent.zeno.schemas
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; Keeping schemas in a single cljc namespace simplifies Avro namespace mgmt

(def actor-id-schema l/string-schema)
(def add-id-schema l/string-schema)
(def authenticator-name-schema l/keyword-schema)
(def branch-schema l/string-schema)
(def chunk-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def cluster-member-id-schema l/string-schema)
(def env-name-schema l/string-schema)
(def fingerprint-schema l/bytes-schema)
(def login-session-token-schema l/string-schema)
(def node-id-schema l/string-schema)
(def state-provider-name-schema l/keyword-schema)
(def timestamp-ms-schema l/long-schema)
(def tx-i-schema l/long-schema)
(def tx-id-schema l/string-schema)
(def tx-log-id-schema l/string-schema)
(def ws-url-schema l/string-schema)

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

;;;;;;;;;;;;;;;;; CRDT Schemas ;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema crdt-op-type-schema
  :add-array-edge
  :add-value
  :delete-array-edge
  :delete-value)

(l/def-record-schema crdt-array-edge-schema
  [:head-node-id node-id-schema]
  [:tail-node-id node-id-schema])

(l/def-record-schema crdt-op-schema
  "Depending on the op-type, different fields will be used."
  [:add-id add-id-schema]
  [:norm-path path-schema]
  [:op-type crdt-op-type-schema]
  [:serialized-value serialized-value-schema]
  [:sys-time-ms timestamp-ms-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(l/def-enum-schema tx-log-type-schema
  :branch :consumer :producer)

(l/def-record-schema tx-log-status-schema
  [:last-tx-i tx-i-schema]
  [:log-type tx-log-type-schema])

(l/def-record-schema tx-log-range-info-schema
  [:end-tx-i "inclusive" tx-i-schema]
  [:log-type tx-log-type-schema]
  [:start-tx-i tx-i-schema])

(l/def-record-schema update-info-schema
  [:norm-path path-schema]
  [:op command-op-schema]
  [:serialized-value serialized-value-schema])

(l/def-record-schema sync-session-info-schema
  [:branch branch-schema]
  [:client-id client-id-schema])

(l/def-record-schema tx-info-schema
  [:actor-id actor-id-schema]
  [:crdt-ops (l/array-schema crdt-op-schema)]
  [:sys-time-ms timestamp-ms-schema]
  [:update-infos (l/array-schema update-info-schema)])

(l/def-record-schema tx-log-segment-schema
  [:head-tx-i tx-i-schema]
  [:next-segment-id tx-log-id-schema]
  [:tx-ids (l/array-schema tx-id-schema)])

;;;;;;;;;;;;;;; Authentication ;;;;;;;;;;;;;;;;

(l/def-record-schema login-session-info-schema
  [:login-session-token login-session-token-schema]
  [:login-session-token-minutes-remaining l/int-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema login-session-token-info-schema
  [:login-session-token-expiration-time-ms l/long-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema log-in-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:branch branch-schema]
  [:serialized-login-info serialized-value-schema])

(l/def-record-schema log-in-ret-schema
  [:serialized-extra-info serialized-value-schema]
  [:login-session-info login-session-info-schema])

(l/def-record-schema update-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:branch branch-schema]
  [:serialized-update-info serialized-value-schema]
  [:update-type l/keyword-schema])

(l/def-record-schema get-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:branch branch-schema]
  [:serialized-get-info serialized-value-schema]
  [:get-type l/keyword-schema])

;;;;;;;;;;;;;;; Envs ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema authenticator-info-schema
  [:authenticator-name authenticator-name-schema]
  [:authenticator-branch branch-schema])

(l/def-record-schema state-provider-info-schema
  [:path-root l/keyword-schema]
  [:state-provider-name state-provider-name-schema]
  [:state-provider-branch branch-schema])

(l/def-record-schema env-info-schema
  [:authenticator-infos (l/array-schema authenticator-info-schema)]
  [:env-name env-name-schema]
  [:state-provider-infos (l/array-schema state-provider-info-schema)])

(l/def-record-schema temp-env-info-schema
  [:env-name env-name-schema]
  [:lifetime-mins l/int-schema]
  [:source-env-name env-name-schema])

;;;;;;;;;;;;;;; RPCs ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema unauthorized-schema
  :zeno/unauthorized)

(l/def-record-schema rpc-arg-schema
  [:rpc-name-kw-ns l/string-schema]
  [:rpc-name-kw-name l/string-schema]
  [:arg serialized-value-schema])

(def rpc-ret-schema
  (l/union-schema [l/null-schema unauthorized-schema serialized-value-schema]))

;;;;;;;;;;;;;;; Talk2 Protocols ;;;;;;;;;;;;;;;;;;;;;

(def admin-client-server-protocol
  {:create-env {:arg-schema env-info-schema
                :ret-schema l/boolean-schema}
   :create-temporary-env {:arg-schema temp-env-info-schema
                          :ret-schema l/boolean-schema}
   :delete-env {:arg-schema env-name-schema
                :ret-schema l/boolean-schema}
   :get-env-names {:arg-schema l/null-schema
                   :ret-schema (l/array-schema env-name-schema)}
   :log-in {:arg-schema l/string-schema
            :ret-schema l/boolean-schema}})

(def client-server-protocol
  {:get-log-range {:arg-schema tx-log-range-info-schema
                   :ret-schema (l/array-schema tx-id-schema)}
   :get-log-status {:arg-schema tx-log-type-schema
                    :ret-schema tx-log-status-schema}
   :get-schema-pcf-for-fingerprint {:arg-schema fingerprint-schema
                                    :ret-schema (l/maybe l/string-schema)}
   :get-tx-info {:arg-schema tx-id-schema
                 :ret-schema tx-info-schema}
   :log-in {:arg-schema log-in-arg-schema
            :ret-schema (l/maybe log-in-ret-schema)}
   :log-out {:arg-schema l/null-schema
             :ret-schema l/boolean-schema}
   :publish-log-status {:arg-schema tx-log-status-schema}
   :resume-login-session {:arg-schema login-session-token-schema
                          :ret-schema (l/maybe login-session-info-schema)}
   :rpc {:arg-schema rpc-arg-schema
         :ret-schema rpc-ret-schema}
   :set-sync-session-info {:arg-schema sync-session-info-schema
                           :ret-schema l/boolean-schema}
   :update-authenticator-state {:arg-schema
                                update-authenticator-state-arg-schema
                                :ret-schema
                                serialized-value-schema}
   :get-authenticator-state {:arg-schema
                             get-authenticator-state-arg-schema
                             :ret-schema
                             serialized-value-schema}})
