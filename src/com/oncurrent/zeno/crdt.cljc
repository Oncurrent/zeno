(ns com.oncurrent.zeno.crdt
  (:require
   [com.oncurrent.zeno.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn apply-ops [{:keys [crdt ops schema sys-time-ms] :as arg}]
  (apply-ops/apply-ops arg))

(defn get-value-info [{:keys [crdt make-id path prefix schema] :as arg}]
  (-> (assoc arg :norm-path (if prefix
                              [prefix]
                              []))
      (c/get-value-info)))

(defn get-value [{:keys [crdt make-id path prefix schema] :as arg}]
  (-> (get-value-info arg)
      (:value)))
