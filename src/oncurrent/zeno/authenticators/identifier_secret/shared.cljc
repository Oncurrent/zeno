(ns oncurrent.zeno.authenticators.identifier-secret.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def authenticator-name :oncurrent.zeno.authenticators/identifier-secret)
(def identifier-schema l/string-schema)
(def secret-schema l/string-schema)

(l/def-record-schema create-actor-info-schema
  [:identifier identifier-schema]
  [:secret secret-schema]
  [:actor-id schemas/actor-id-schema])

(l/def-record-schema login-info-schema
  [:identifier identifier-schema]
  [:secret secret-schema])

(l/def-record-schema set-secret-info-schema
  [:old-secret secret-schema]
  [:new-secret secret-schema])

(l/def-record-schema session-token-info-schema
  [:expiration-time-ms schemas/timestamp-ms-schema]
  [:actor-id schemas/actor-id-schema])
