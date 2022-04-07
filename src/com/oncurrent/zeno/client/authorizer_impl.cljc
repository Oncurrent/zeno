(ns com.oncurrent.zeno.client.authorizer-impl
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IClientAuthorizer
  (allowed? [this actor path path-actor op])
  (get-name [this]))
