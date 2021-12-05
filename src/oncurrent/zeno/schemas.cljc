(ns oncurrent.zeno.schemas
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; Keeping schemas in a single cljc namespace simplifies Avro namespace mgmt

(def authenticator-name-schema l/string-schema)
(def branch-id-schema l/string-schema)
(def client-id-schema l/string-schema)
(def fingerprint-schema l/bytes-schema)
(def id-schema l/string-schema)
(def subject-id-schema l/string-schema)
(def timestamp-ms-schema l/long-schema)
(def session-token-schema l/string-schema)

(l/def-record-schema serialized-value-schema
  [:bytes l/bytes-schema]
  [:fp fingerprint-schema])

(l/def-record-schema mutex-info-schema
  [:lease-id l/string-schema]
  [:lease-length-ms l/int-schema]
  [:owner l/string-schema])

(l/def-record-schema branch-info-schema
  [:authenticators (l/array-schema authenticator-name-schema)]
  [:source-tx-id id-schema]
  [:use-temp-storage l/boolean-schema])

(def branch-name-to-info-schema
  (l/map-schema branch-info-schema))

;;;;;;;;;;;;;;;;; CRDT Schemas ;;;;;;;;;;;;;;;;;;;;;;;

(l/def-enum-schema command-op-schema
  :add-identifier-to-subject
  :add-subjects-to-acl
  :add-subjects-to-group
  :change-secret-for-subject
  :create-subject
  :delete-subject
  :divide
  :insert-after
  :insert-before
  :insert-range-after
  :insert-range-before
  :minus
  :mod
  :multiply
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

(l/def-record-schema tx-info-schema
  [:branch-id branch-id-schema]
  [:cmds (l/array-schema serializable-command-schema)]
  [:subject-id subject-id-schema]
  [:ts timestamp-ms-schema])

(l/def-record-schema tx-log-block-schema
  [:prev-log-block-id id-schema]
  [:tx-i l/int-schema]
  [:tx-id id-schema])

(l/def-record-schema crdt-value-info-schema
  [:deletion-tx-id id-schema]
  [:value serialized-value-schema])

(l/def-record-schema crdt-value-node-schema
  [:add-id-to-value-info (l/map-schema crdt-value-info-schema)]
  [:current-add-ids (l/array-schema id-schema)]
  [:tx-ids (l/array-schema id-schema)])

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
