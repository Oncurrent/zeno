(ns integration.test-schemas
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def rpcs
  {:add-nums {:arg-schema (l/array-schema l/int-schema)
              :ret-schema l/int-schema}})
