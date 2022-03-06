(ns oncurrent.zeno.authorization
  (:require
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IClientAuthorizer
  (allowed? [this actor path path-actor op]))
