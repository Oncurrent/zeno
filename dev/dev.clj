(ns dev
  (:require [clojure.data :as data]
            [com.oncurrent.zeno.state-providers.crdt :as crdt]
            [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
            [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
            [com.oncurrent.zeno.state-providers.crdt.get :as get]
            [com.oncurrent.zeno.utils :as u]
            [deercreeklabs.lancaster :as l]
            [taoensso.timbre :as log]))

(def data-schema (l/map-schema
                  (l/array-schema l/int-schema)))

(def ret (commands/process-cmds
          {:cmds [{:zeno/arg 1
                   :zeno/op :zeno/insert-before
                   :zeno/path [:crdt "a" 0]}
                  {:zeno/arg 2
                   :zeno/op :zeno/insert-before
                   :zeno/path [:crdt "a" 0]}]
           :data-schema data-schema
           :root :crdt}))

(def ao-crdt (apply-ops/apply-ops
              {:crdt-ops (:crdt-ops ret)
               :data-schema data-schema
               :root :crdt}))

(= (:crdt ret) ao-crdt)

(get/get-in-state
 {:crdt #_ao-crdt (:crdt ret)
  :data-schema data-schema
  :path [:crdt]
  :root :crdt})
