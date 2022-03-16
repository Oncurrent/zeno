(ns com.oncurrent.zeno.client.authorization
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IClientAuthorizer
  (allowed? [this actor path path-actor op])
  (get-name [this]))
