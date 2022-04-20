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

(def tx-info-prefix "_TX-INFO-FOR-TX-ID-")

(defn tx-id->tx-info-k [tx-id]
  (str tx-info-prefix tx-id))

(defn <ser-tx-info->tx-info
  [{:keys [schema ser-tx-info storage] :as fn-arg}]
  (au/go
    (let [crdt-ops (au/<? (common/<serializable-crdt-ops->crdt-ops
                           (assoc fn-arg :ops (:crdt-ops ser-tx-info))))
          update-infos (au/<? (common/<serializable-update-infos->update-infos
                               (assoc fn-arg
                                      :update-infos
                                      (:update-infos ser-tx-info))))]
      (-> ser-tx-info
          (assoc :crdt-ops crdt-ops)
          (assoc :update-infos update-infos)))))

(defn <ser-tx-infos->tx-infos
  [{:keys [ser-tx-infos] :as fn-arg}]
  (au/go
    (if (empty? ser-tx-infos)
      []
      (let [last-i (dec (count ser-tx-infos))]
        (loop [i 0
               out []]
          (let [ser-tx-info (nth ser-tx-infos i)
                tx-info (au/<? (<ser-tx-info->tx-info
                                (assoc fn-arg :ser-tx-info ser-tx-info)))
                new-out (conj out tx-info)]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defn make-handlers [{:keys [*<send-msg *storage schema]}]
  {:record-txs
   (fn [{:keys [<request-schema conn-id env-info] :as h-arg}]
     (au/go
       (let [branch (-> env-info :env-sp-root->info :zeno/crdt
                        :state-provider-branch)
             {:keys [env-name]} env-info
             _ (log/info (str "HHHH:\n"
                              (u/pprint-str
                               (u/sym-map env-name conn-id branch))))

             storage @*storage
             ser-tx-infos (:arg h-arg)
             ;; Convert the serializable-tx-infos into regular tx-infos
             ;; so we can ensure that we have any required schemas
             ;; (which will be requested from the client).
             tx-infos (au/<? (<ser-tx-infos->tx-infos
                              (u/sym-map <request-schema schema
                                         ser-tx-infos storage)))]
         (doseq [{:keys [tx-id] :as ser-tx-info} ser-tx-infos]
           (let [k (tx-id->tx-info-k tx-id)]
             ;; Use <swap! instead of <add! so we can handle
             ;; repeated idempotent syncs.
             (au/<? (storage/<swap! storage k shared/tx-info-schema
                                    (fn [old-ser-tx-info]
                                      ser-tx-info)))))
         true)))})

(defn ->state-provider
  [{::crdt/keys [authorizer schema]}]
  (let [*<send-msg (atom nil)
        *branch->crdt-store (atom {})
        *storage (atom nil)
        <get-state (fn [{:zeno/keys [branch path]}]
                     (au/go
                       (common/get-value
                        {:crdt (get @*branch->crdt-store branch)
                         :path path
                         :schema schema})))
        <update-state! (fn [{:zeno/keys [branch cmds] :as us-arg}]
                         (au/go
                           (swap! *branch->crdt-store
                                  update branch
                                  (fn [old-crdt]
                                    (let [pc-arg {:cmds cmds
                                                  :crdt old-crdt
                                                  :schema schema}]
                                      (-> (commands/process-cmds pc-arg)
                                          (:crdt)))))
                           true))
        <copy-branch! (fn [{:keys [state-provider-branch
                                   state-provider-branch-source]}]
                        (log/info (str "CCCC:\n"
                                       (u/pprint-str
                                        (u/sym-map
                                         state-provider-branch
                                         state-provider-branch-source)))))
        arg (u/sym-map *storage *<send-msg schema)
        msg-handlers (make-handlers arg)
        init! (fn [{:keys [<send-msg storage]}]
                (reset! *storage storage)
                (reset! *<send-msg <send-msg)
                ;; TODO: Load *branch->crdt-store from logs/snapshots
                )]
    #::sp-impl{:<copy-branch! <copy-branch!
               :<get-state <get-state
               :<update-state! <update-state!
               :init! init!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :state-provider-name shared/state-provider-name}))
