(ns com.oncurrent.zeno.state-providers.crdt.get
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti get-in-state* (fn [{:keys [schema]}]
                          (c/schema->dispatch-type schema)))
(defmulti get-crdt-info* (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmethod get-in-state* :single-value
  [{:keys [crdt-info]}]
  (let [[[add-id value-info] & rest] (:add-id-to-value-info crdt-info)]
    (when (seq rest)
      (throw (ex-info "CRDT needs repair" crdt-info)))
    (:value value-info)))

(defmethod get-crdt-info* :single-value
  [{:keys [crdt-info]}]
  (let [{:keys [add-id-to-value-info]} crdt-info]
    (reduce-kv (fn [acc add-id {:keys [sys-time-ms]}]
                 (assoc acc add-id sys-time-ms))
               {}
               (:add-id-to-value-info crdt-info))))

(defn get-in-state [{:keys [crdt data-schema] :as arg}]
  (get-in-state* (assoc arg
                        :schema data-schema
                        :crdt-info (:crdt-info crdt))))

(defn get-crdt-info [{:keys [crdt data-schema] :as arg}]
  (get-crdt-info* (assoc arg
                         :schema data-schema
                         :crdt-info (:crdt-info crdt))))
