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

(defn ->unsynced-log-k [{:keys [env-name]}]
  (str unsynced-log-prefix env-name))

(defn get-actor-id-str [actor-id]
  (if (= :unauthenticated actor-id)
    "UNAUTHENTICATED"
    actor-id))

(defn <get-unsynced-log [{:keys [*env-name *storage]}]
  (let [k (->unsynced-log-k {:env-name @*env-name})]
    (storage/<get @*storage k shared/unsynced-log-schema)))

(defn get-unauthorized-commands [{:keys [actor-id authorizer cmds]}]
  (reduce (fn [acc {:zeno/keys [path op] :as cmd}]
            ;; TODO: Replace nil with the actor-id of the path's writer
            (if (authz-impl/allowed? authorizer actor-id path nil op)
              acc
              (conj acc cmd)))
          []
          cmds))

(defn throw-not-initialized! [{:keys [root]}]
  (throw (ex-info (str "State provider for root `" root
                       "` is not initialized.")
                  (u/sym-map root))))

(defn <wait-for-init [{:keys [*storage]}]
  (au/go
    (loop []
      (if @*storage
        true
        (do
          (ca/<! (ca/timeout 50))
          (recur))))))

(defn make-<update-state!
  [{:keys [*actor-id *crdt-state *env-name *storage *v
           authorizer client-id root schema signal-producer-sync!]
    :as arg}]
  (fn [{:zeno/keys [cmds] :keys [root]}]
    (au/go
      (au/<? (<wait-for-init arg))
      (let [actor-id @*actor-id
            env-name @*env-name
            unauth-cmds (get-unauthorized-commands
                         (u/sym-map actor-id authorizer cmds))]
        (if (seq unauth-cmds)
          (throw (ex-info "Transaction aborted due to unauthorized cmds."
                          (u/sym-map cmds unauth-cmds actor-id)))
          (let [ret (commands/process-cmds {:cmds cmds
                                            :crdt @*crdt-state
                                            :schema schema
                                            :root root})
                {:keys [crdt ops update-infos]} ret
                ;; We can use `reset!` here because there are no
                ;; concurrent updates
                _ (reset! *crdt-state crdt)
                _ (swap! *v (fn [v]
                              (common/update-v
                               (u/sym-map crdt root schema update-infos v))))
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
                         :update-infos ser-update-infos}
                unsynced-log-k (->unsynced-log-k (u/sym-map env-name))]
            (au/<? (storage/<swap! storage k shared/serializable-tx-info-schema
                                   (constantly tx-info)))
            (au/<? (storage/<swap! storage unsynced-log-k
                                   shared/unsynced-log-schema
                                   (fn [aid->tx-ids]
                                     (let [aid (get-actor-id-str actor-id)]
                                       (update aid->tx-ids aid
                                               conj tx-id)))))
            (signal-producer-sync!)
            update-infos))))))

(defn <sync-producer-txs-for-actor-id!
  [{:keys [*client-running? *connected? *host-fns
           actor-id storage sync-whole-log? unsynced-log-k]}]
  (let [{:keys [<send-msg]} @*host-fns
        actor-id-str (get-actor-id-str actor-id)]
    (au/go
      (loop []
        (let [aid->tx-ids (au/<? (storage/<get storage
                                               unsynced-log-k
                                               shared/unsynced-log-schema))
              tx-ids (get aid->tx-ids actor-id-str)
              batch (set (take 10 tx-ids))
              tx-infos (when tx-ids
                         (au/<? (common/<get-serializable-tx-infos
                                 {:storage storage
                                  :tx-ids batch})))]
          (when (seq tx-infos)
            (au/<? (<send-msg {:msg-type :log-txs
                               :arg tx-infos}))
            (au/<? (storage/<swap! storage
                                   unsynced-log-k
                                   shared/unsynced-log-schema
                                   (fn [aid->tx-ids]
                                     (update aid->tx-ids actor-id-str
                                             #(reduce (fn [acc tx-id]
                                                        (if (batch tx-id)
                                                          acc
                                                          (conj acc tx-id)))
                                                      []
                                                      %))))))
          (when (and @*client-running?
                     @*connected?
                     sync-whole-log?
                     (> (count tx-ids) (count batch)))
            (recur)))))))

(defn <sync-producer-txs!*
  [{:keys [*actor-id *connected? *env-name *storage] :as arg}]
  (au/go
    (au/<? (<wait-for-init arg))
    (when @*connected?
      (let [actor-id @*actor-id
            actor-id-str (get-actor-id-str actor-id)
            storage @*storage
            unsynced-log-k (->unsynced-log-k {:env-name @*env-name})
            aid->tx-ids (au/<? (storage/<get storage
                                             unsynced-log-k
                                             shared/unsynced-log-schema))
            aids (reduce-kv (fn [acc aid tx-ids]
                              (cond
                                (empty? tx-ids)
                                acc

                                ;; Put current actor-id at the front
                                (= actor-id-str aid)
                                (vec (cons actor-id acc))

                                :else
                                (conj acc aid)))
                            []
                            aid->tx-ids)]
        ;; Sync the whole log for the current actor-id, then sync
        ;; batches any other logs
        (doseq [aid aids]
          (au/<? (<sync-producer-txs-for-actor-id!
                  (assoc arg
                         :actor-id aid
                         :storage storage
                         :sync-whole-log? (= actor-id aid)
                         :unsynced-log-k unsynced-log-k))))))))

(defn <load-snapshot! [{:keys [snapshot-info]}]
  (au/go
    (let [{:keys [url]} snapshot-info]
      ;; TODO: Fetch and apply snapshot
      )))

(defn <sync-consumer-txs!*
  [{:keys [*actor-id *client-running? *connected? *crdt-state *host-fns
           *last-tx-i *storage *v root schema]
    :as arg}]
  (au/go
    (au/<? (<wait-for-init arg))
    (when @*connected?
      (let [actor-id @*actor-id
            storage @*storage
            {:keys [<request-schema <send-msg update-subscriptions!]} @*host-fns
            msg-arg  {:msg-type :get-consumer-sync-info
                      :arg {:last-tx-i @*last-tx-i}}
            sync-info (when @*connected? (au/<? (<send-msg msg-arg)))
            {:keys [last-snapshot-info tx-ids-since-snapshot]} sync-info]
        (when (and last-snapshot-info @*connected?)
          (au/<? (<load-snapshot!
                  (assoc arg :snapshot-info last-snapshot-info))))
        ;; TODO: Finish this part about getting and applying tx-infos
        #_(when (and (seq tx-ids-since-snapshot) @*connected?)
          (let [tx-infos (-> tx-ids-since-snapshot
                             (filter :already-have)
                             (<send-msg :tx-ids->tx-infos)
                             (au/<?)
                             (common/<serializable-tx-infos->tx-infos))
                ; tx-infos (au/<? (common/<serializable-tx-infos->tx-infos
                ;                  (u/sym-map <request-schema schema
                ;                             tx-infos-since-snapshot storage)))
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
            (swap! *v (fn [v]
                        (update-v {:crdt @*crdt-state
                                   :root root
                                   :schema schema
                                   :update-infos (:update-infos info)
                                   :v v})))
            (reset! *last-tx-i xxx)
            (update-subscriptions! (:update-infos info))))))))

(defn load-local-data! [{:keys [*host-fns signal-consumer-sync!] :as arg}]
  (ca/go
    (au/<? (<wait-for-init arg))
    (try
      (let [{:keys [update-subscriptions!]} @*host-fns
            update-infos []]
        ;; TODO: Implement
        (update-subscriptions! update-infos)
        (signal-consumer-sync!))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error (str "Error in load-local-data!:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn make-get-in-state [{:keys [*v]}]
  (fn get-in-state [{:keys [path root] :as gis-arg}]
    (let [path (u/chop-root path root)]
      (reduce (fn [{:keys [value] :as acc} k]
                (let [[k* value*] (cond
                                   (or (keyword? k) (nat-int? k) (string? k))
                                   [k (when value
                                        (get value k))]

                                   (and (int? k) (neg? k))
                                   (let [arg {:array-len (count value)
                                              :i k}
                                         i (u/get-normalized-array-index arg)]
                                     [i (nth value i)])

                                   (nil? k)
                                   [nil nil]

                                   :else
                                   (throw-bad-path-key path k))]
                  (-> acc
                      (update :norm-path conj k*)
                      (assoc :value value*))))
              {:norm-path []
               :value @*v}
              path))))

(defn ->state-provider
  [{::crdt/keys [authorizer schema root] :as config}]
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
  (let [*actor-id (atom :unauthenticated)
        *client-running? (atom true)
        *connected? (atom false)
        *crdt-state (atom nil)
        *env-name (atom nil)
        *host-fns (atom {})
        *last-tx-i (atom nil)
        *storage (atom nil)
        *v (atom nil)
        client-id (u/compact-random-uuid)
        init! (fn [{:keys [<request-schema <send-msg
                           connected? env-name storage update-subscriptions!]}]
                (reset! *env-name env-name)
                (reset! *host-fns (u/sym-map <request-schema
                                             <send-msg
                                             connected?
                                             update-subscriptions!))
                (reset! *storage storage))
        sync-arg (u/sym-map *actor-id *client-running? *connected? *crdt-state
                            *host-fns *env-name *last-tx-i *storage *v
                            root schema)
        psync-ret (u/start-task-loop!
                   {:loop-delay-ms 3000
                    :loop-name "<sync-producer-txs!"
                    :task-fn #(<sync-producer-txs!* sync-arg)})
        csync-ret (u/start-task-loop!
                   {:loop-delay-ms 3000
                    :loop-name "<sync-consumer-txs!"
                    :task-fn #(<sync-consumer-txs!* sync-arg)})
        {stop-producer-sync! :stop!
         signal-producer-sync! :now!} psync-ret
        {stop-consumer-sync! :stop!
         signal-consumer-sync! :now!} csync-ret
        mus-arg (u/sym-map *actor-id *crdt-state *env-name *host-fns *storage *v
                           authorizer client-id root schema
                           signal-producer-sync!)
        <wait-for-sync (fn [& args]
                         (let [actor-id (or (some-> args first :actor-id)
                                            @*actor-id)]
                           (au/go
                             (loop []
                               (let [aid->tx-ids (au/<? (<get-unsynced-log
                                                         sync-arg))
                                     actor-id-str (get-actor-id-str actor-id)
                                     tx-ids (get aid->tx-ids actor-id-str)]
                                 (if (empty? tx-ids)
                                   true
                                   (do
                                     (ca/<! (ca/timeout 50))
                                     (recur))))))))
        lld-arg (assoc sync-arg :signal-consumer-sync! signal-consumer-sync!)
        <on-actor-id-change (fn [actor-id]
                              (au/go
                                (reset! *actor-id actor-id)
                                (reset! *crdt-state nil)
                                (reset! *v nil)
                                (reset! *last-tx-i nil)
                                (load-local-data! lld-arg)))
        stop! (fn []
                (reset! *client-running? false)
                (stop-producer-sync!)
                (stop-consumer-sync!))]
    (load-local-data! lld-arg)
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :<wait-for-sync <wait-for-sync
               :get-in-state (make-get-in-state (u/sym-map *v))
               :init! init!
               :msg-handlers {:notify-consumer-log-sync
                              (fn [arg]
                                (signal-consumer-sync!))}
               :msg-protocol shared/msg-protocol
               :<on-actor-id-change <on-actor-id-change
               :on-connect (fn [url]
                             (reset! *connected? true))
               :on-disconnect (fn [code]
                                (reset! *connected? false))
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
