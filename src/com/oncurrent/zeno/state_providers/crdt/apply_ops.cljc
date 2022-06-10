(ns com.oncurrent.zeno.state-providers.crdt.apply-ops
  (:require
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as impl]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.repair]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn apply-ops
  [{:keys [crdt crdt-ops schema] :as arg}]
  (let [crdt* (impl/apply-ops-without-repair arg)]
    (c/repair (assoc arg :crdt crdt*))))
