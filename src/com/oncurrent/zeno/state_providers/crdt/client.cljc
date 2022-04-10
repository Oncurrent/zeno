(ns com.oncurrent.zeno.state-providers.crdt.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.crdt :as crdt]
   [com.oncurrent.zeno.crdt.commands :as crdt-commands]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-<update-state! [{:keys [*crdt-state crdt-schema]}]
  (fn [{:zeno/keys [cmds]}]
    (au/go
      (let [ret (crdt-commands/process-cmds {:cmds cmds
                                             :crdt @*crdt-state
                                             :crdt-schema crdt-schema})
            {:keys [crdt ops update-infos]} ret]
        ;; We can use `reset!` here b/c there are not concurrent updates
        (reset! *crdt-state crdt)
        ;; TODO: log the ops
        update-infos))))

(defn state-provider
  [{:keys [authorizer crdt-schema]}]
  ;; TODO: Check args
  ;; TODO: Load initial state from IDB
  (let [*crdt-state (atom nil)
        arg (u/sym-map *crdt-state authorizer crdt-schema)
        get-in-state (fn [{:keys [path] :as arg}]
                       (crdt/get-value-info (assoc arg
                                                   :crdt @*crdt-state
                                                   :path (rest path)
                                                   :schema crdt-schema)))]
    (assoc arg
           :<update-state! (make-<update-state! arg)
           :get-in-state get-in-state
           :get-state-atom (constantly *crdt-state))))
