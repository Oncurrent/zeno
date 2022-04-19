(ns com.oncurrent.zeno.state-providers.crdt.server
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(defn ->state-provider
  [{::crdt/keys [authorizer schema]}]
  (let [*branch->crdt-store (atom {})
        <get-state (fn [{:zeno/keys [branch path]}]
                     (au/go
                       (common/get-value
                        {:crdt (get @*branch->crdt-store branch)
                         :path (rest path)
                         :schema schema})))
        <update-state! (fn [{:zeno/keys [branch cmds] :as arg}]
                         (au/go
                           (swap! *branch->crdt-store
                                  update branch
                                  (fn [old-crdt]
                                    (let [arg {:cmds cmds
                                               :crdt old-crdt
                                               :schema schema}]
                                      (-> (commands/process-cmds arg)
                                          (:crdt)))))
                           true))
        <set-state! (fn [{:zeno/keys [branch path value]}]
                      (<update-state! #:zeno{:branch branch
                                             :cmds [#:zeno{:arg value
                                                           :op :zeno/set
                                                           :path path}]}))
        msg-handlers {:add-nums (fn [{:keys [arg conn-id env-name] :as h-arg}]
                                  (apply + arg))}]
    #::sp-impl{:<get-state <get-state
               :<set-state! <set-state!
               :<update-state! <update-state!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :state-provider-name shared/state-provider-name}))
