(ns com.oncurrent.zeno.client.authorizer-impl
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IClientAuthorizer
  (allowed? [this actor-id path path-actor-id op])
  (get-name [this]))
