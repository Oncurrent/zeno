(ns com.oncurrent.zeno.authenticators.magic-token.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]))

(def authenticator-name :com.oncurrent.zeno.authenticators/magic-token)

(def token-schema l/string-schema)
(def identifier-schema l/string-schema)

(l/def-record-schema serialized-value-schema
  [:bytes l/bytes-schema]
  [:fp schemas/fingerprint-schema])

(l/def-record-schema request-magic-token-info-schema
  [:identifier identifier-schema]
  [:mins-valid l/int-schema]
  [:number-of-uses l/int-schema]
  [:serialized-extra-info serialized-value-schema])

(l/def-record-schema magic-token-info-schema
  [:actor-id schemas/actor-id-schema]
  [:identifier identifier-schema]
  [:expiration-ms l/long-schema]
  [:remaining-uses l/int-schema]
  [:serialized-extra-info serialized-value-schema])

(l/def-record-schema create-actor-info-schema
  [:identifier identifier-schema]
  [:actor-id schemas/actor-id-schema])


