(ns oncurrent.zeno.crdt
  (:require
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.get-value :as get-value]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn apply-ops [{:keys [crdt ops schema sys-time-ms] :as arg}]
  (apply-ops/apply-ops arg))

(defn get-value [{:keys [crdt make-add-id path schema] :as arg}]
  (get-value/get-value arg))
