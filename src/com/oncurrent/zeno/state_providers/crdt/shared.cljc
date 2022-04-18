(ns com.oncurrent.zeno.state-providers.crdt.shared
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def state-provider-name :com.oncurrent.zeno.state-providers/crdt)

(def msg-protocol
  {:add-nums {:arg-schema (l/array-schema l/int-schema)
              :ret-schema l/int-schema}
   :notify {:arg-schema l/string-schema}})
