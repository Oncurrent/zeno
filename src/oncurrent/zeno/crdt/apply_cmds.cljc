(ns oncurrent.zeno.crdt.apply-cmds
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.common :as c]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti apply-cmd (fn [{:keys [schema]}]
                      (c/schema->dispatch-type schema)))

(defmethod apply-cmd :single-value
  [{:keys [cmd crdt]}]
  (let []
    {:crdt crdt
     :ops #{{:add-id "a1"
             :op-type :add-value
             :path []
             :value "ABC"}}}))

(defn apply-cmds
  [{:keys [cmds crdt] :as arg}]
  (reduce (fn [acc cmd]
            (let [ret (apply-cmd (assoc arg :cmd cmd :crdt acc))]
              (-> acc
                  (assoc :crdt (:crdt ret))
                  (update :ops set/union (:ops ret)))))
          {:crdt crdt
           :ops #{}}
          cmds))
