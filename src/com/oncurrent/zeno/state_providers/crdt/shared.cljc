(ns com.oncurrent.zeno.state-providers.crdt.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def state-provider-name :com.oncurrent.zeno.state-providers/crdt)

(def add-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def node-id-schema l/string-schema)
(def tx-i-schema l/long-schema)
(def tx-id-schema l/string-schema)

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
  [:norm-path schemas/path-schema]
  [:op-type crdt-op-type-schema]
  [:serialized-value schemas/serialized-value-schema]
  [:sys-time-ms schemas/timestamp-ms-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(l/def-record-schema update-info-schema
  [:norm-path schemas/path-schema]
  [:op schemas/command-op-schema]
  [:serialized-value schemas/serialized-value-schema])

(l/def-record-schema tx-info-schema
  [:actor-id schemas/actor-id-schema]
  [:client-id client-id-schema]
  [:crdt-ops (l/array-schema crdt-op-schema)]
  [:sys-time-ms schemas/timestamp-ms-schema]
  [:tx-id tx-id-schema]
  [:update-infos (l/array-schema update-info-schema)])

(def unsynced-log-schema (l/array-schema tx-id-schema))

;;;;;;;;;;;;;;;; Msg Protocol ;;;;;;;;;;;;;;;;;;;;;

(def msg-protocol
  {:record-txs {:arg-schema (l/array-schema tx-info-schema)
                :ret-schema l/boolean-schema}})
