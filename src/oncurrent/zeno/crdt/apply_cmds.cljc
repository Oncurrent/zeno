(ns oncurrent.zeno.crdt.apply-cmds
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.common :as c]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn apply-cmd [{:keys [cmd crdt make-add-id]}]
  {:crdt crdt
   :ops #{{:add-id "a1"
           :op-type :add-value
           :path []
           :value "ABC"}}})

(defn apply-cmds
  [{:keys [cmds crdt make-add-id]
    :or {make-add-id u/compact-random-uuid}}]
  (reduce (fn [acc cmd]
            (let [ret (apply-cmd {:cmd cmd
                                  :crdt (:crdt acc)
                                  :make-add-id make-add-id})]
              (-> acc
                  (assoc :crdt (:crdt ret))
                  (update :ops set/union (:ops ret)))))
          {:crdt crdt
           :ops #{}}
          cmds))
