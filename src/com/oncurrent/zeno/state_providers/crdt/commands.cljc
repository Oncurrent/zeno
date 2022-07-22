(ns com.oncurrent.zeno.state-providers.crdt.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def insert-crdt-ops #{:zeno/insert-after
                       :zeno/insert-before
                       :zeno/insert-range-after
                       :zeno/insert-range-before})

(defmulti process-cmd* (fn [{:keys [cmd]}]
                         (let [{:zeno/keys [op]} cmd]
                           (if (insert-crdt-ops op)
                             :zeno/insert*
                             op))))

(defmulti get-delete-ops (fn [{:keys [data-schema]}]
                           (c/schema->dispatch-type data-schema)))

(defmulti get-add-ops (fn [{:keys [data-schema]}]
                        (c/schema->dispatch-type data-schema)))

(defmethod get-delete-ops :single-value
  [{:keys [crdt op-path] :as arg}]
  (reduce (fn [acc add-id]
            (conj acc {:add-id add-id
                       :op-type :delete-value
                       :op-path op-path}))
          #{}
          (keys (get/get-crdt-info (assoc arg :path op-path)))))

(defmethod get-add-ops :single-value
  [{:keys [cmd-arg crdt make-id op-path sys-time-ms value-schema] :as arg}]
  #{{:add-id (make-id)
     :op-type :add-value
     :op-path op-path
     :serialized-value (l/serialize value-schema cmd-arg)
     :sys-time-ms (or sys-time-ms (u/current-time-ms))
     :value cmd-arg}})

(defmethod process-cmd* :zeno/set
  [arg]
  (let [crdt-ops (set/union (get-delete-ops arg)
                            (get-add-ops arg))
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defn process-cmd [{:keys [cmd data-schema root] :as arg}]
  (let [cmd-path (:zeno/path cmd)
        op-path (u/chop-root cmd-path root)
        value-schema  (l/schema-at-path data-schema op-path)]
    (process-cmd* (-> arg
                      (assoc :cmd-arg (:zeno/arg cmd))
                      (assoc :cmd-path cmd-path)
                      (assoc :cmd-type (:zeno/op cmd))
                      (assoc :op-path op-path)
                      (assoc :value-schema value-schema)))))

(defn process-cmds [{:keys [cmds crdt make-id root] :as arg}]
  (let [make-id* (or make-id u/compact-random-uuid)]
    (reduce (fn [acc cmd]
              (let [ret (process-cmd (assoc arg
                                            :crdt (:crdt acc)
                                            :cmd cmd
                                            :make-id make-id*))]
                (-> acc
                    (assoc :crdt (:crdt ret))
                    (update :crdt-ops set/union (:crdt-ops ret)))))
            {:crdt crdt
             :crdt-ops #{}}
            cmds)))
