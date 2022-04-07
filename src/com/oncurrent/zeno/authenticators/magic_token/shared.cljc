(ns com.oncurrent.zeno.authenticators.magic-token.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]))

(def authenticator-name :com.oncurrent.zeno.authenticators/magic-token)

(def magic-token-schema l/string-schema)
(def identifier-schema l/string-schema)

(l/def-record-schema serialized-value-schema
  [:bytes l/bytes-schema]
  [:fp schemas/fingerprint-schema])

(def unlimited :unlimited)
(l/def-enum-schema unlimited-schema
  unlimited)

(l/def-union-schema long-or-unlimited-schema
  l/long-schema
  unlimited-schema)

(l/def-record-schema request-magic-token-info-schema
  [:identifier identifier-schema]
  [:mins-valid long-or-unlimited-schema]
  [:number-of-uses long-or-unlimited-schema]
  [:serialized-extra-info serialized-value-schema]
  [:serialized-params serialized-value-schema])

(l/def-record-schema magic-token-info-schema
  [:actor-id schemas/actor-id-schema]
  [:identifier identifier-schema]
  [:expiration-ms long-or-unlimited-schema]
  [:remaining-uses long-or-unlimited-schema]
  [:requestor-id schemas/actor-id-schema]
  [:serialized-extra-info serialized-value-schema])

(l/def-record-schema create-actor-info-schema
  [:identifier identifier-schema]
  [:actor-id schemas/actor-id-schema])
