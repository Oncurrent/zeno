(ns com.oncurrent.zeno.client.authorization
  (:require
   [com.oncurrent.zeno.authorization :as authz]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defrecord AffirmativeClientAuthorizer []
  authz/IClientAuthorizer
  (allowed? [this actor path path-actor op]
    true))

(defn make-affirmative-authorizer []
  (->AffirmativeClientAuthorizer))
