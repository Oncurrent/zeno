(ns com.oncurrent.zeno.authenticators.password.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def authenticator-name :com.oncurrent.zeno.authenticators/password)
(def identifier-schema l/string-schema)
(def password-schema l/string-schema)

(l/def-record-schema add-actor-and-password-info-schema
  [:actor-id schemas/actor-id-schema]
  [:password password-schema])

(l/def-record-schema login-info-schema
  [:actor-id schemas/actor-id-schema]
  [:password password-schema])

(l/def-record-schema set-password-info-schema
  [:actor-id schemas/actor-id-schema]
  [:new-password password-schema]
  [:old-password password-schema])
