(ns com.oncurrent.zeno.client.state-provider-impl
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <send-sp-msg!
  [{:keys [arg
           msg-type-name
           timeout-ms
           zeno-client
           ]}])
