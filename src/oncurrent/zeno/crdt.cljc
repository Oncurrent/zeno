(ns oncurrent.zeno.crdt
  (:require
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.common :as c]
   [oncurrent.zeno.crdt.process-cmds :as pc]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn apply-ops [{:keys [crdt ops schema sys-time-ms] :as arg}]
  (apply-ops/apply-ops arg))

(defn get-value [{:keys [crdt make-id path schema] :as arg}]
  (c/get-value arg))

(defn process-cmds [{:keys [cmds crdt make-id path schema] :as arg}]
  (pc/process-cmds arg))
