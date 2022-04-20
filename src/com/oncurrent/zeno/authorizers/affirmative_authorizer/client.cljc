(ns com.oncurrent.zeno.authorizers.affirmative-authorizer.client
  (:require
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.shared :as shared]
   [com.oncurrent.zeno.client.authorizer-impl :as client-authz]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defrecord AffirmativeClientAuthorizer []
  client-authz/IClientAuthorizer
  (allowed? [this actor-id path path-actor-id op]
    true)
  (get-name [this]
    shared/authorizer-name))

(defn ->authorizer []
  (->AffirmativeClientAuthorizer))
