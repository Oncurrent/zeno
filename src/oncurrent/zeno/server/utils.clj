(ns oncurrent.zeno.server.utils
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-schema-requester [{:keys [conn-id server]}]
  (fn [fp]
    (let [arg {:arg fp
               :conn-id conn-id
               :msg-type-name :get-schema-pcf-for-fingerprint
               :server server}]
      (t2s/<send-msg! arg))))
