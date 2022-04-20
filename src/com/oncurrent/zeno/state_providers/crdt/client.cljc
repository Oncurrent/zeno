(ns com.oncurrent.zeno.state-providers.crdt.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.authorizer-impl :as authz-impl]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))


(def unsynced-log-prefix "_UNSYNCED-LOG-")

(defn actor-id->unsynced-log-k [actor-id]
  (str unsynced-log-prefix (if (= :unauthenticated actor-id)
                             "UNAUTHENTICATED"
                             actor-id)))

(defn get-unauthorized-commands [{:keys [actor-id authorizer cmds]}]
  (reduce (fn [acc {:zeno/keys [path op] :as cmd}]
            ;; TODO: Replace nil with the actor-id of the path's writer
            (if (authz-impl/allowed? authorizer actor-id path nil op)
              acc
              (conj acc cmd)))
          []
          cmds))

(defn make-<update-state!
  [{:keys [*actor-id *crdt-state *storage
           authorizer client-id new-tx-ch schema]}]
  (fn [{:zeno/keys [cmds] :keys [prefix]}]
    (au/go
      (let [actor-id @*actor-id
            unauth-cmds (get-unauthorized-commands
                         (u/sym-map actor-id authorizer cmds))]
        (if (seq unauth-cmds)
          (throw (ex-info "Transaction aborted due to unauthorized cmds."
                          (u/sym-map cmds unauth-cmds actor-id)))
          (let [ret (commands/process-cmds {:cmds cmds
                                            :crdt @*crdt-state
                                            :schema schema
                                            :prefix prefix})
                {:keys [crdt ops update-infos]} ret
                ;; We can use `reset!` here because there are no
                ;; concurrent updates
                _ (reset! *crdt-state crdt)
                tx-id (u/compact-random-uuid)
                k (common/tx-id->tx-info-k tx-id)
                storage @*storage
                ser-ops (au/<? (common/<crdt-ops->serializable-crdt-ops
                                (u/sym-map ops schema storage)))
                ser-update-infos (au/<?
                                  (common/<update-infos->serializable-update-infos
                                   (u/sym-map schema storage update-infos)))
                tx-info {:actor-id actor-id
                         :client-id client-id
                         :crdt-ops ser-ops
                         :sys-time-ms (u/current-time-ms)
                         :tx-id tx-id
                         :update-infos ser-update-infos}]
            (au/<? (storage/<swap! storage k shared/serializable-tx-info-schema
                                   (constantly tx-info)))
            (au/<? (storage/<swap! storage
                                   (actor-id->unsynced-log-k actor-id)
                                   shared/unsynced-log-schema
                                   (fn [old-log]
                                     (conj (or old-log []) tx-id))))
            (ca/>! new-tx-ch true)
            update-infos))))))

(defn <sync-unsynced-log!
  [{:keys [*client-running? *sync-session-running?
           <send-msg actor-id storage]}]
  (au/go
    (loop []
      (let [unsynced-log-k (actor-id->unsynced-log-k actor-id)
            tx-ids (au/<? (storage/<get storage
                                        unsynced-log-k
                                        shared/unsynced-log-schema))
            batch (set (take 10 tx-ids))
            tx-infos (when tx-ids
                       (->> {:storage storage :tx-ids batch}
                            (common/<get-tx-infos)
                            (au/<?)
                            (filter #(= actor-id (:actor-id %)))))]
        (when (seq tx-infos)
          (do
            (au/<? (<send-msg {:msg-type :log-txs
                               :arg tx-infos}))
            (au/<? (storage/<swap! storage
                                   unsynced-log-k
                                   shared/unsynced-log-schema
                                   (fn [old-log]
                                     (reduce (fn [acc tx-id]
                                               (if (batch tx-id)
                                                 acc
                                                 tx-id))
                                             []
                                             old-log))))))
        (when (and @*client-running?
                   @*sync-session-running?
                   (< (count batch) (count tx-ids)))
          (recur))))))

(defn <consume-txs!
  [{:keys [*crdt-state *last-tx-id <request-schema <send-msg schema storage
           update-subscriptions!]}]
  ;; TODO: Implement batching
  (au/go
    (let [msg-arg  {:msg-type :get-consumer-txs
                    :arg {:last-tx-id @*last-tx-id}}
          serializable-tx-infos (au/<? (<send-msg msg-arg))]
      (when (seq serializable-tx-infos)
        (let [tx-infos (au/<? (common/<serializable-tx-infos->tx-infos
                               (u/sym-map <request-schema schema
                                          serializable-tx-infos storage)))
              info (reduce (fn [acc {:keys [crdt-ops tx-id update-infos]}]
                             (-> acc
                                 (assoc :last-tx-id tx-id)
                                 (update :ops concat crdt-ops)
                                 (update :update-infos concat update-infos)))
                           {:last-tx-id nil
                            :ops []
                            :update-infos []}
                           tx-infos)]
          (swap! *crdt-state (fn [old-crdt]
                               (apply-ops/apply-ops {:crdt old-crdt
                                                     :ops (:ops info)
                                                     :schema schema})))
          (reset! *last-tx-id (:last-tx-id info))
          (update-subscriptions! (:update-infos info)))))))

(defn sync-producer-txs!
  [{:keys [*client-running? *sync-session-running? new-tx-ch] :as fn-arg}]
  (ca/go
    (try
      (reset! *sync-session-running? true)
      (loop []
        (au/<? (<sync-unsynced-log! fn-arg))
        (au/alts? [new-tx-ch (ca/timeout 2000)])
        (when (and @*client-running?
                   @*sync-session-running?)
          (recur)))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error in sync-producer-txs!:\n"
                   (u/ex-msg-and-stacktrace e))))))

(defn sync-consumer-txs!
  [{:keys [*client-running? *sync-session-running? server-tx-ch] :as fn-arg}]
  (ca/go
    (try
      (reset! *sync-session-running? true)
      (loop []
        (au/<? (<consume-txs! fn-arg))
        (au/alts? [server-tx-ch (ca/timeout 100)])
        (when (and @*client-running?
                   @*sync-session-running?)
          (recur)))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error in sync-consumer-txs!:\n"
                   (u/ex-msg-and-stacktrace e))))))

(defn start-sync-session!
  [{:keys [*client-running? *sync-session-running? new-tx-ch] :as fn-arg}]
  (when-not @*sync-session-running?
    (sync-producer-txs! fn-arg)
    (sync-consumer-txs! fn-arg)))

(defn end-sync-session! [{:keys [*sync-session-running?]}]
  (reset! *sync-session-running? false))

(defn ->state-provider
  [{::crdt/keys [authorizer schema] :as config}]
  ;; TODO: Check args
  ;; TODO: Load initial state from IDB
  (when-not authorizer
    (throw (ex-info (str "No authorizer was specified in the state provider "
                         "configuration.")
                    config)))

  (when-not schema
    (throw (ex-info (str "No schema was specified in the state provider "
                         "configuration.")
                    config)))
  (let [*crdt-state (atom nil)
        *actor-id (atom :unauthenticated)
        get-in-state (fn [{:keys [path prefix] :as gs-arg}]
                       (common/get-value-info
                        (assoc gs-arg
                               :crdt @*crdt-state
                               :path (common/chop-root path prefix)
                               :norm-path [prefix]
                               :schema schema)))
        *host-fns (atom {})
        *client-running? (atom true)
        *storage (atom nil)
        *sync-session-running? (atom false)
        *last-tx-id (atom nil)
        client-id (u/compact-random-uuid)
        init! (fn [{:keys [<request-schema <send-msg
                           connected? storage update-subscriptions!]}]
                (reset! *host-fns (u/sym-map <request-schema
                                             <send-msg
                                             connected?
                                             update-subscriptions!))
                (reset! *storage storage))
        new-tx-ch (ca/chan (ca/dropping-buffer 1))
        ;; TODO: Connect this to the server
        server-tx-ch (ca/chan (ca/dropping-buffer 1))
        mus-arg (u/sym-map *actor-id *crdt-state *host-fns *storage
                           authorizer client-id new-tx-ch schema)
        ->sync-arg #(let [actor-id @*actor-id
                          storage @*storage
                          {:keys [<request-schema
                                  <send-msg
                                  connected?
                                  update-subscriptions!]} @*host-fns]
                      (u/sym-map actor-id
                                 *client-running?
                                 *crdt-state
                                 *last-tx-id
                                 *sync-session-running?
                                 <request-schema
                                 <send-msg
                                 connected?
                                 new-tx-ch
                                 schema
                                 server-tx-ch
                                 storage
                                 update-subscriptions!))
        <wait-for-sync (fn []
                         (au/go
                           (loop []
                             (let [k (actor-id->unsynced-log-k @*actor-id)
                                   tx-ids (au/<? (storage/<get
                                                  @*storage k
                                                  shared/unsynced-log-schema))]
                               (when (seq tx-ids)
                                 (ca/<! (ca/timeout 10))
                                 (recur))))))
        on-actor-id-change (fn [actor-id]
                             (let [sync-arg (->sync-arg)]
                               (reset! *actor-id actor-id)
                               (reset! *crdt-state nil)
                               (reset! *last-tx-id nil)
                               (end-sync-session! sync-arg)
                               (start-sync-session! sync-arg)))
        stop! (fn []
                (reset! *client-running? false)
                (end-sync-session! (->sync-arg)))]
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :<wait-for-sync <wait-for-sync
               :get-in-state get-in-state
               :get-state-atom (constantly *crdt-state)
               :init! init!
               :msg-handlers {}
               :msg-protocol shared/msg-protocol
               :on-actor-id-change on-actor-id-change
               :on-connect (fn [url]
                             (start-sync-session! (->sync-arg)))
               :on-disconnect (fn [code]
                                (end-sync-session! (->sync-arg)))
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
