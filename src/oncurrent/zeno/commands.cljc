(ns oncurrent.zeno.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.commands :as crdt-commands]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.sharing :as sharing]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti process-cmd (fn [{:keys [cmd]}]
                        (first (:path cmd))))

(defmethod process-cmd :zeno/client
  [arg]
  :foo)

(defmethod process-cmd :zeno/sharing
  [arg]
  (sharing/process-cmd arg))

(defmethod process-cmd :zeno/crdt
  [arg]
  (let [arg* (-> arg
                 (update-in [:cmd :path] rest)
                 (assoc :crdt (:crdt-store arg))
                 (assoc :schema (:crdt-store-schema arg)))
        ret (crdt-commands/process-cmd arg*)]
    (-> arg
        (assoc :crdt-store (:crdt ret))
        (update :ops set/union (:ops ret)))) )

(defmethod process-cmd :default
  [{:keys [cmd]}]
  (when-not cmd
    (throw (ex-info (str "The `:cmd` value is missing from the argument to "
                         "`process-cmd`.")
                    {})))
  (throw (ex-info (str "Unknown path root `" (first (:path cmd))
                       "` in update command.")
                  cmd)))

(defn process-cmds [{:keys [cmds make-id]
                     :or {make-id u/compact-random-uuid}
                     :as arg}]
  (reduce (fn [acc cmd]
            (process-cmd (assoc acc
                                :cmd cmd
                                :make-id make-id)))
          arg
          cmds))
