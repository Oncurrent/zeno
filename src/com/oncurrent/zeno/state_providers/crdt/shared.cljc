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
  :add-container
  :add-value
  :delete-array-edge
  :delete-container
  :delete-value)

(l/def-record-schema crdt-array-edge-schema
  [:head-node-id node-id-schema]
  [:tail-node-id node-id-schema])

(l/def-record-schema serializable-crdt-op-schema
  "Depending on the op-type, different fields will be used."
  [:add-id add-id-schema]
  [:op-type crdt-op-type-schema]
  [:op-path schemas/path-schema]
  [:serialized-value schemas/serialized-value-schema]
  [:sys-time-ms schemas/timestamp-ms-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(def client-id-schema l/string-schema)
(def tx-index-schema l/int-schema)
(def tx-id-schema l/string-schema)

(l/def-record-schema serializable-tx-info-schema
  [:actor-id schemas/actor-id-schema]
  [:client-id client-id-schema]
  [:crdt-ops (l/array-schema serializable-crdt-op-schema)]
  [:sys-time-ms schemas/timestamp-ms-schema]
  [:tx-id tx-id-schema]
  [:updated-paths (l/array-schema schemas/path-schema)])

(def unsynced-log-schema
  (l/map-schema ;; keys are actor-ids
   (l/array-schema tx-id-schema)))

(l/def-record-schema serializable-snapshot-schema
  [:edn-crdt-info l/string-schema])

(l/def-record-schema actor-log-info-schema
  [:branch-log-tx-indices-since-snapshot (l/array-schema tx-index-schema)]
  [:snapshot-tx-index tx-index-schema]
  [:snapshot-txs-hash l/long-schema])

(l/def-record-schema branch-log-info-schema
  [:actor-id-to-log-info (l/map-schema actor-log-info-schema)]
  [:branch-tx-ids (l/array-schema tx-id-schema)])

;;;;;;;;;;;;;;;; Msg Protocol ;;;;;;;;;;;;;;;;;;;;;

(l/def-record-schema get-consumer-sync-info-arg-schema
  [:last-tx-index tx-index-schema])

(l/def-record-schema get-consumer-sync-info-ret-schema
  [:snapshot-url l/string-schema]
  [:snapshot-tx-index tx-index-schema]
  [:tx-ids-since-snapshot (l/array-schema tx-id-schema)])

(l/def-record-schema get-tx-infos-arg-schema
  [:tx-ids (l/array-schema tx-id-schema)])

(def msg-protocol
  {:get-consumer-sync-info {:arg-schema get-consumer-sync-info-arg-schema
                            :ret-schema get-consumer-sync-info-ret-schema}
   :get-tx-infos {:arg-schema get-tx-infos-arg-schema
                  :ret-schema (l/array-schema serializable-tx-info-schema)}
   :log-producer-tx-batch {:arg-schema (l/array-schema
                                        serializable-tx-info-schema)
                           :ret-schema l/boolean-schema}
   :notify-consumer-log-sync {:arg-schema l/null-schema}})
