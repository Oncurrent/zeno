(ns com.oncurrent.zeno.authorization
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IClientAuthorizer
  (allowed? [this actor path path-actor op]))
