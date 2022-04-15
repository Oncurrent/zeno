(ns com.oncurrent.zeno.server.authorizer-impl
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IServerAuthorizer
  (allowed? [this actor path path-actor op])
  (get-name [this]))
