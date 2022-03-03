(ns oncurrent.zeno.schemas
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; Keeping schemas in a single cljc namespace simplifies Avro namespace mgmt

(def authenticator-name-schema l/keyword-schema)
(def branch-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def cluster-member-id-schema l/string-schema)
(def fingerprint-schema l/bytes-schema)
(def id-schema l/string-schema)
(def session-token-schema l/string-schema)
(def actor-id-schema l/string-schema)
(def timestamp-ms-schema l/long-schema)
(def ws-url-schema l/string-schema)

(l/def-record-schema serialized-value-schema
  [:bytes l/bytes-schema]
  ;; TODO: Move this to another schema?
  [:chunk-ids "Used for chunked storage" (l/array-schema id-schema)]
  [:fp fingerprint-schema])

(l/def-record-schema chunk-schema
  [:bytes l/bytes-schema]
  [:chunk-i l/int-schema])

;;;;;;;;;;;;;;;; Server Schemas ;;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema branch-info-schema
  [:authenticators (l/array-schema authenticator-name-schema)]
  [:source-tx-id id-schema]
  [:use-temp-storage l/boolean-schema])

(def branch-name-to-info-schema
  (l/map-schema branch-info-schema))

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

(l/def-record-schema serializable-command-schema
  [:zeno/arg serialized-value-schema]
  [:zeno/op command-op-schema]
  [:zeno/path path-schema])

;;;;;;;;;;;;;;;;; CRDT Schemas ;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema crdt-op-type-schema
  :add-array-edge
  :add-value
  :delete-array-edge
  :delete-value)

(l/def-record-schema crdt-op-schema
  "Depending on the op-type, different fields will be used."
  [:add-id id-schema]
  [:op-type crdt-op-type-schema]
  [:path path-schema]
  [:serialized-value serialized-value-schema]
  [:sys-time-ms timestamp-ms-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(l/def-record-schema tx-info-schema
  [:crdt-ops (l/array-schema crdt-op-schema)]
  [:actor-id actor-id-schema]
  [:sys-time-ms timestamp-ms-schema]
  [:update-cmds (l/array-schema serializable-command-schema)])

(l/def-record-schema tx-log-block-schema
  [:prev-log-block-id id-schema]
  [:tx-i
   "Useful for getting the tx-i of the tail without walking the whole log"
   l/int-schema]
  [:tx-id id-schema]
  [:tx-info tx-info-schema])


;;;;;;;;;;;;;;; Authentication ;;;;;;;;;;;;;;;;

(l/def-record-schema session-info-schema
  [:session-token session-token-schema]
  [:session-token-minutes-remaining l/int-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema session-token-info-schema
  [:session-token-expiration-time-ms l/long-schema]
  [:actor-id actor-id-schema])

(l/def-record-schema log-in-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:branch-id branch-id-schema]
  [:serialized-login-info serialized-value-schema])

(l/def-record-schema log-in-ret-schema
  [:serialized-extra-info serialized-value-schema]
  [:session-info session-info-schema])

(l/def-record-schema update-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:branch-id branch-id-schema]
  [:serialized-update-info serialized-value-schema]
  [:update-type l/keyword-schema])

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

(def client-server-protocol
  {:get-schema-pcf-for-fingerprint {:arg fingerprint-schema
                                    :ret (l/maybe l/string-schema)}
   :log-in {:arg log-in-arg-schema
            :ret (l/maybe log-in-ret-schema)}
   :log-out {:arg l/null-schema
             :ret l/boolean-schema}
   :resume-session {:arg session-token-schema
                    :ret (l/maybe session-info-schema)}
   :rpc {:arg rpc-arg-schema
         :ret rpc-ret-schema}
   :update-authenticator-state {:arg update-authenticator-state-arg-schema
                                :ret serialized-value-schema}})
