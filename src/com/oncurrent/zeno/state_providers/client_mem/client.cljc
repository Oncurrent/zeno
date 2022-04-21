(ns com.oncurrent.zeno.state-providers.client-mem.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.client-mem :as client-mem]
   [com.oncurrent.zeno.state-providers.client-mem.shared :as shared]
   [com.oncurrent.zeno.state-providers.client-mem.commands :as commands]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-<update-state! [{:keys [*state]}]
  (fn [{:zeno/keys [cmds] :keys [root]}]
    (au/go
     (let [ret (commands/eval-cmds @*state cmds root)
           {:keys [state update-infos]} ret]
        ;; We can use `reset!` here b/c there are no concurrent updates
        (reset! *state state)
        update-infos))))

(defn ->state-provider
  ([] (->state-provider nil))
  ([{::client-mem/keys [initial-state]}]
   (let [*state (atom initial-state)]
     #::sp-impl{:<update-state! (make-<update-state! (u/sym-map *state))
                :get-in-state (fn [{:keys [root path]}]
                                (commands/get-in-state @*state path root))
                :get-state-atom (constantly *state)
                :state-provider-name shared/state-provider-name})))
