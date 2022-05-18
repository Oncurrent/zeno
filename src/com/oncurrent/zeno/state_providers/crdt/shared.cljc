(ns com.oncurrent.zeno.state-providers.crdt.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def state-provider-name :com.oncurrent.zeno.state-providers/crdt)

;;;;;;;;;;;;;;;;; CRDT Schemas ;;;;;;;;;;;;;;;;;;;;;;;

(def add-id-schema l/string-schema)
(def node-id-schema l/string-schema)

(l/def-enum-schema crdt-op-type-schema
  :add-array-edge
  :add-value
  :delete-array-edge
  :delete-value)

(l/def-record-schema crdt-array-edge-schema
  [:head-node-id node-id-schema]
  [:tail-node-id node-id-schema])

(l/def-record-schema serializable-crdt-op-schema
  "Depending on the op-type, different fields will be used."
  [:add-id add-id-schema]
  [:norm-path schemas/path-schema]
  [:op-type crdt-op-type-schema]
  [:path schemas/path-schema]
  [:serialized-value schemas/serialized-value-schema]
  [:sys-time-ms schemas/timestamp-ms-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(def client-id-schema l/string-schema)
(def tx-id-schema l/string-schema)

(l/def-record-schema serializable-update-info-schema
  [:norm-path schemas/path-schema]
  [:op schemas/command-op-schema]
  [:serialized-value schemas/serialized-value-schema])

(l/def-record-schema serializable-tx-info-schema
  [:actor-id schemas/actor-id-schema]
  [:client-id client-id-schema]
  [:crdt-ops (l/array-schema serializable-crdt-op-schema)]
  [:sys-time-ms schemas/timestamp-ms-schema]
  [:tx-id tx-id-schema]
  [:update-infos (l/array-schema serializable-update-info-schema)])

(def unsynced-log-schema
  (l/map-schema ;; keys are actor-ids
   (l/array-schema tx-id-schema)))

(l/def-record-schema segmented-log-schema
  [:parent-log-k l/string-schema]
  [:tx-ids (l/array-schema tx-id-schema)])

;;;;;;;;;;;;;;;; Msg Protocol ;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema snapshot-schema
  [:serialized-crdt l/string-schema]
  [:serialized-value schemas/serialized-value-schema])

(l/def-record-schema snapshot-info-schema
  [:fp schemas/fingerprint-schema]
  [:last-tx-i l/int-schema]
  [:s3-key l/string-schema]
  [:url l/string-schema])

(l/def-record-schema get-consumer-sync-info-arg-schema
  [:last-tx-i l/int-schema])

(l/def-record-schema get-consumer-sync-info-ret-schema
  [:last-snapshot-info snapshot-info-schema]
  [:tx-ids-since-snapshot (l/array-schema tx-id-schema)])

(def msg-protocol
  {:get-consumer-sync-info {:arg-schema get-consumer-sync-info-arg-schema
                            :ret-schema get-consumer-sync-info-ret-schema}
   :log-txs {:arg-schema (l/array-schema serializable-tx-info-schema)
             :ret-schema l/boolean-schema}
   :notify-consumer-log-sync {:arg-schema l/null-schema}})
