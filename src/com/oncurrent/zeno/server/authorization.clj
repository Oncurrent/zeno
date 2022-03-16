(ns com.oncurrent.zeno.server.authorization
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IServerAuthorizer
  (allowed? [this actor path path-actor op])
  (get-name [this]))
