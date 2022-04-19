(ns com.oncurrent.zeno.state-providers.crdt.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.authorizer-impl :as authz-impl]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def tx-info-prefix "TX-INFO-FOR-TX-ID-")
(def unsynced-log-prefix "_UNSYNCED-LOG-")

(defn tx-id->tx-info-k [tx-id]
  (str tx-info-prefix tx-id))

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
                ;; We can use `reset!` here because there are no concurrent updates
                _ (reset! *crdt-state crdt)
                tx-id (u/compact-random-uuid)
                k (tx-id->tx-info-k tx-id)
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
            (au/<? (storage/<add! storage k shared/tx-info-schema tx-info))
            (au/<? (storage/<swap! storage
                                   (actor-id->unsynced-log-k actor-id)
                                   shared/unsynced-log-schema
                                   (fn [old-log]
                                     (conj (or old-log []) tx-id))))
            (ca/>! new-tx-ch true)
            update-infos))))))

(defn <get-tx-info [{:keys [storage tx-id]}]
  (au/go
    (let [tx-info (au/<? (storage/<get storage
                                       (tx-id->tx-info-k tx-id)
                                       shared/tx-info-schema))]
      (assoc tx-info :tx-id tx-id))))

(defn <get-tx-infos [{:keys [storage tx-ids]}]
  (au/go
    (if (zero? (count tx-ids))
      []
      (let [chs (map #(<get-tx-info {:storage storage :tx-id %})
                     tx-ids)
            ret-ch (ca/merge chs)
            last-i (dec (count tx-ids))]
        (loop [i 0
               out []]
          (let [new-out (conj out (au/<? ret-ch))]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defn <sync-unsynced-log!
  [{:keys [*client-running? *sync-session-running? <send-msg actor-id storage]}]
  (au/go
    (loop []
      (let [unsynced-log-k (actor-id->unsynced-log-k actor-id)
            tx-ids (au/<? (storage/<get storage
                                        unsynced-log-k
                                        shared/unsynced-log-schema))
            batch (set (take 10 tx-ids))
            tx-infos (when tx-ids
                       (->> {:storage storage :tx-ids batch}
                            (<get-tx-infos)
                            (au/<?)
                            (filter #(= actor-id (:actor-id %)))))]
        (when (seq tx-infos)
          (au/<? (<send-msg {:msg-type :record-txs
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
                                           old-log)))))
        (when (and @*client-running?
                   @*sync-session-running?
                   (< (count batch) (count tx-ids)))
          (recur))))))

(defn start-sync-session!
  [{:keys [*client-running? *sync-session-running? new-tx-ch] :as arg}]
  (when-not @*sync-session-running?
    (ca/go
      (try
        (reset! *sync-session-running? true)
        (loop []
          (au/<? (<sync-unsynced-log! arg))
          (au/alts? [new-tx-ch (ca/timeout 2000)])
          (when (and @*client-running?
                     @*sync-session-running?)
            (recur)))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in start-sync-session!:\n"
                     (u/ex-msg-and-stacktrace e)))))))

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
        *sync-session-running? (atom false)
        *storage (atom nil)
        client-id (u/compact-random-uuid)
        init! (fn [{:keys [<send-msg connected? storage]}]
                (reset! *host-fns (u/sym-map <send-msg connected?))
                (reset! *storage storage))
        new-tx-ch (ca/chan (ca/dropping-buffer 1))
        mus-arg (u/sym-map *actor-id *crdt-state *host-fns *storage
                           authorizer client-id new-tx-ch schema)
        ->sync-arg #(let [actor-id @*actor-id
                          storage @*storage
                          {:keys [<send-msg connected?]} @*host-fns]
                      (u/sym-map actor-id
                                 *client-running?
                                 *sync-session-running?
                                 <send-msg
                                 connected?
                                 new-tx-ch
                                 storage))
        stop! (fn []
                (reset! *client-running? false)
                (end-sync-session! (->sync-arg)))]
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :get-in-state get-in-state
               :get-state-atom (constantly *crdt-state)
               :init! init!
               :msg-handlers {}
               :msg-protocol shared/msg-protocol
               :on-actor-id-change (fn [actor-id]
                                     (let [sync-arg (->sync-arg)]
                                       (reset! *actor-id actor-id)
                                       (end-sync-session! sync-arg)
                                       (start-sync-session! sync-arg)))
               :on-connect (fn [url]
                             (start-sync-session! (->sync-arg)))
               :on-disconnect (fn [code]
                                (end-sync-session! (->sync-arg)))
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
