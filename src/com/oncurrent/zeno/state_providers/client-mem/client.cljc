(ns com.oncurrent.zeno.state-providers.client-mem.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-<update-state! [{:keys [*crdt-state schema]}]
  (fn [{:zeno/keys [cmds]}]
    (au/go
      (let [ret (commands/process-cmds {:cmds cmds
                                        :crdt @*crdt-state
                                        :crdt-schema schema})
            {:keys [crdt ops update-infos]} ret]
        ;; We can use `reset!` here b/c there are no concurrent updates
        (reset! *crdt-state crdt)
        ;; TODO: log the ops
        update-infos))))

(defn ->state-provider
  [arg]
  ;; Move
  ;; com.oncurrent.zeno.client.client-commands to
  ;; com.oncurrent.zeno.state-providers.client-mem.commands
  ;; Then use `commands/get-in-state` and `commands/eval-cmd`
  ;; See `unit.client-commands-test` examples

  (let [get-in-state (fn [{:keys [path] :as arg}]
                       (common/get-value-info (assoc arg
                                                     :crdt @*crdt-state
                                                     :path (rest path)
                                                     :schema schema)))]
    #::sp-impl{:<update-state! (make-<update-state!
                                (u/sym-map *crdt-state schema))
               :get-in-state get-in-state
               :get-name (constantly shared/state-provider-name)
               :get-state-atom (constantly *crdt-state)}))
