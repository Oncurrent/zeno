(ns com.oncurrent.zeno.authorizers.affirmative-authorizer.server
  (:require
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.shared :as shared]
   [com.oncurrent.zeno.server.authorizer-impl :as server-authz]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defrecord AffirmativeServerAuthorizer []
  server-authz/IServerAuthorizer
  (allowed? [this actor path path-actor op]
    true)
  (get-name [this]
    shared/authorizer-name))

(defn make-affirmative-authorizer []
  (->AffirmativeServerAuthorizer))
