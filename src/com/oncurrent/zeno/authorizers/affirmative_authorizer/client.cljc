(ns com.oncurrent.zeno.authorizers.affirmative-authorizer.client
  (:require
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.shared :as shared]
   [com.oncurrent.zeno.client.authorization :as client-authz]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defrecord AffirmativeClientAuthorizer []
  client-authz/IClientAuthorizer
  (allowed? [this actor path path-actor op]
    true)
  (get-name [this]
    shared/authorizer-name))

(defn make-affirmative-authorizer []
  (->AffirmativeClientAuthorizer))
