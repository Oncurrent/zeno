(ns com.oncurrent.zeno.state-providers.crdt.apply-ops
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti apply-op (fn [{:keys [op-type value-schema]}]
                     [(c/schema->dispatch-type value-schema) op-type]))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt op-path sys-time-ms] :as arg}]
  (if (some-> (:deleted-add-ids crdt)
              (get add-id))
    crdt
    (let [value (or (:value arg)
                    (c/deserialize-op-value arg))
          value-info (u/sym-map sys-time-ms value)]
      (update-in crdt
                 [:crdt-info :add-id-to-value-info]
                 assoc add-id value-info))))

(defmethod apply-op [:single-value :delete-value]
  [{:keys [add-id crdt op-path] :as arg}]
  (if (some-> (:deleted-add-ids crdt)
              (get add-id))
    crdt
    (-> crdt
        (update-in [:crdt-info :add-id-to-value-info] dissoc add-id)
        (update :deleted-add-ids #(conj (or % #{}) add-id)))))

(defn apply-ops
  [{:keys [crdt crdt-ops data-schema] :as arg}]
  (reduce (fn [acc {:keys [sys-time-ms op-path] :as op}]
            (let [value-schema (l/schema-at-path data-schema op-path)]
              (apply-op (assoc op
                               :crdt acc
                               :value-schema value-schema
                               :sys-time-ms (or sys-time-ms
                                                (:sys-time-ms arg)
                                                (u/current-time-ms))))))
          crdt
          crdt-ops))
