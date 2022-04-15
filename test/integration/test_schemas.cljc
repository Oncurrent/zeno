(ns integration.test-schemas
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(l/def-record-schema crdt-schema
  [:name l/string-schema]
  [:numbers (l/array-schema l/int-schema)])

(def rpcs
  {:add-nums {:arg-schema (l/array-schema l/int-schema)
              :ret-schema l/int-schema}
   :get-name {:arg-schema l/null-schema
              :ret-schema (l/maybe l/string-schema)}
   :remove-name {:arg-schema l/null-schema
                 :ret-schema l/boolean-schema}
   :set-name {:arg-schema l/string-schema
              :ret-schema l/boolean-schema}
   :throw-if-even {:arg-schema l/int-schema
                   :ret-schema l/boolean-schema}})
