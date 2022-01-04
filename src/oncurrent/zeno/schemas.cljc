(ns oncurrent.zeno.schemas
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; Keeping schemas in a single cljc namespace simplifies Avro namespace mgmt

(def authenticator-name-schema l/string-schema)
(def branch-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def cluster-member-id-schema l/string-schema)
(def fingerprint-schema l/bytes-schema)
(def id-schema l/string-schema)
(def session-token-schema l/string-schema)
(def subject-id-schema l/string-schema)
(def timestamp-ms-schema l/long-schema)
(def ws-url-schema l/string-schema)

(l/def-record-schema serialized-value-schema
  [:chunk-ids (l/array-schema id-schema)]
  [:fp fingerprint-schema]
  [:bytes l/bytes-schema])

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
  :add-identifier-to-subject
  :add-subjects-to-acl
  :add-subjects-to-group
  :change-secret-for-subject
  :create-subject
  :delete-subject
  :insert-after
  :insert-before
  :insert-range-after
  :insert-range-before
  :remove
  :remove-identifier-from-subject
  :remove-subjects-from-acl
  :remove-subjects-from-group
  :set
  :set-acl
  :set-authenticator-info)

(l/def-record-schema modify-group-arg-schema
  [:group-subject-id id-schema]
  [:member-subject-ids (l/array-schema id-schema)])

(l/def-union-schema path-item-schema
  l/keyword-schema
  l/string-schema
  l/long-schema)

(l/def-array-schema path-schema
  path-item-schema)

(l/def-record-schema serializable-command-schema
  [:arg serialized-value-schema]
  [:op command-op-schema]
  [:path path-schema])

;;;;;;;;;;;;;;;;; CRDT Schemas ;;;;;;;;;;;;;;;;;;;;;;;

#_
(l/def-record-schema crdt-value-info-schema
  [:serialized-value serialized-value-schema]
  [:sys-time-ms timestamp-ms-schema]
  [:union-branch l/int-schema])
#_
(l/def-record-schema set-crdt-schema
  [:current-add-id-to-value-info (l/map-schema crdt-value-info-schema)]
  [:deleted-add-ids l/string-set-schema])
#_
(l/def-record-schema value-history-set-crdt-schema
  [:add-id-to-value-info (l/map-schema crdt-value-info-schema)]
  [:current-add-ids l/string-set-schema]
  [:deleted-add-ids l/string-set-schema])
#_
(l/def-record-schema array-edge-schema
  [:head-node-add-id id-schema]
  [:tail-node-add-id id-schema])

(l/def-enum-schema crdt-op-type-schema
  :add-array-edge
  :add-array-node
  :add-map-key
  :add-map-key-value
  :add-record-key-value
  :add-single-value
  :del-array-edge
  :del-array-node
  :del-map-key
  :del-map-key-value
  :del-record-key-value
  :del-single-value)

(l/def-record-schema crdt-op-schema
  "Depending on the op-type, different fields will be used."
  [:add-id id-schema]
  [:item-id id-schema]
  [:k l/string-schema]
  [:op-type crdt-op-type-schema]
  [:serialized-value serialized-value-schema]
  [:sys-time-ms timestamp-ms-schema]
  [:union-branch l/int-schema])

;;;;;;;;;;;;;;;; Transaction & Log Schemas ;;;;;;;;;;;;;;;;;

(l/def-record-schema tx-info-schema
  [:crdt-ops (l/array-schema crdt-op-schema)]
  [:subject-id subject-id-schema]
  [:sys-time-ms timestamp-ms-schema]
  [:update-cmds (l/array-schema serializable-command-schema)])

(l/def-record-schema tx-log-block-schema
  [:prev-log-block-id id-schema]
  [:tx-i
   "Useful for getting the tx-i of the tail without walking the whole log"
   l/int-schema]
  [:tx-id id-schema]
  [:tx-info tx-info-schema])

;;;;;;;;;;;;;; ACL Schemas ;;;;;;;;;;;;;;;;;

(l/def-enum-schema permissions-schema
  :r :rs :rw :rws)

(l/def-record-schema acl-entry-schema
  [:subject-id id-schema]
  [:permissions permissions-schema])

(l/def-record-schema crdt-acl-entry-info-schema
  [:deletion-tx-id id-schema]
  [:acl-entry acl-entry-schema])

(l/def-record-schema acl-info-schema
  [:acl-add-id-to-info (l/map-schema crdt-acl-entry-info-schema)]
  [:acl-current-add-ids (l/array-schema id-schema)])

(l/def-record-schema crdt-map-key-info-schema
  [:acl-info acl-info-schema]
  [:crdt-reference id-schema])

(l/def-record-schema crdt-map-chunk-schema
  [:key-to-key-info (l/map-schema crdt-map-key-info-schema)])

;;;;;;;;;;;;;;; Authentication ;;;;;;;;;;;;;;;;

(l/def-record-schema session-info-schema
  [:session-token session-token-schema]
  [:subject-id subject-id-schema])

(l/def-record-schema log-in-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:authentication-info serialized-value-schema]
  [:branch-id branch-id-schema])

(l/def-record-schema log-in-ret-schema
  [:session-info session-info-schema]
  [:extra-info serialized-value-schema])

(l/def-record-schema query-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:arg serialized-value-schema])

(l/def-record-schema update-authenticator-state-arg-schema
  [:authenticator-name authenticator-name-schema]
  [:arg serialized-value-schema])

;;;;;;;;;;;;;;; RPCs ;;;;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema unauthorized-schema
  :zeno/unauthorized)

(l/def-record-schema rpc-arg-schema
  [:rpc-name-kw-ns l/string-schema]
  [:rpc-name-kw-name l/string-schema]
  [:arg serialized-value-schema])

(def rpc-ret-schema
  (l/union-schema [l/null-schema unauthorized-schema serialized-value-schema]))

;;;;;;;;;;;;;;; Protocols ;;;;;;;;;;;;;;;;;;;;;

(def client-server-protocol
  {:roles
   [:client :server]

   :msgs
   {:get-schema-pcf-for-fingerprint {:arg fingerprint-schema
                                     :ret (l/maybe l/string-schema)
                                     :sender :either}
    :log-in {:arg log-in-arg-schema
             :ret log-in-ret-schema
             :sender :client}
    :log-out {:arg l/null-schema
              :ret l/boolean-schema
              :sender :client}
    :query-authenticator-state {:arg query-authenticator-state-arg-schema
                                :ret serialized-value-schema
                                :sender :client}
    :resume-session {:arg session-token-schema
                     :ret l/boolean-schema
                     :sender :client}
    :rpc {:arg rpc-arg-schema
          :ret rpc-ret-schema
          :sender :client}
    :set-client-id {:arg client-id-schema
                    :ret true
                    :sender :client}
    :update-authenticator-state {:arg update-authenticator-state-arg-schema
                                 :ret serialized-value-schema
                                 :sender :client}}})
