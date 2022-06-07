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
  (if (= u/unauthenticated-actor-id-kw actor-id)
    u/unauthenticated-actor-id-str
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

(defn update-state-info!
  [{:keys [*state-info crdt last-tx-index root schema snapshot tx-infos]}]
  (let [info (reduce (fn [acc {:keys [crdt-ops updated-paths]}]
                       (-> acc
                           (update :crdt-ops concat crdt-ops)
                           (update :updated-paths concat updated-paths)))
                     {:crdt-ops []
                      :updated-paths []}
                     tx-infos)
        {:keys [crdt-ops updated-paths]} info
        apply-ops (fn [crdt]
                    (apply-ops/apply-ops {:crdt crdt
                                          :crdt-ops crdt-ops
                                          :schema schema}))]
    (swap! *state-info
           (fn [state-info]
             (let [crdt* (or crdt
                             (cond-> (:crdt (or snapshot state-info))
                               (seq tx-infos) (apply-ops)))]
               {:crdt crdt*
                :last-tx-index (or last-tx-index
                                   (:last-tx-index state-info))})))
    (u/sym-map updated-paths)))

(defn make-<update-state!
  [{:keys [*actor-id *env-name *state-info *storage
           authorizer client-id schema signal-producer-sync!]
    :as arg}]
  (fn [{:zeno/keys [cmds]
        :keys [root]
        :as fn-arg}]
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
                                            :crdt (:crdt @*state-info)
                                            :schema schema
                                            :root root})
                {:keys [crdt crdt-ops updated-paths]} ret
                tx-id (u/compact-random-uuid)
                k (common/tx-id->tx-info-k tx-id)
                storage @*storage
                sys-time-ms (u/current-time-ms)
                tx-info (u/sym-map actor-id client-id crdt-ops sys-time-ms
                                   tx-id updated-paths)
                tx-infos [tx-info]
                ser-tx-info (au/<? (common/<tx-info->serializable-tx-info
                                    (u/sym-map schema storage tx-info)))
                _ (update-state-info! (u/sym-map *state-info crdt root schema
                                                 tx-infos))
                unsynced-log-k (->unsynced-log-k (u/sym-map env-name))]
            (au/<? (storage/<swap! storage k shared/serializable-tx-info-schema
                                   (constantly ser-tx-info)))
            (au/<? (storage/<swap! storage unsynced-log-k
                                   shared/unsynced-log-schema
                                   (fn [aid->tx-ids]
                                     (let [aid (get-actor-id-str actor-id)]
                                       (update aid->tx-ids aid
                                               conj tx-id)))))
            (signal-producer-sync!)
            (u/sym-map updated-paths)))))))

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
              ser-tx-infos (when tx-ids
                             (au/<? (common/<get-serializable-tx-infos
                                     {:storage storage
                                      :tx-ids batch})))]
          (when (seq ser-tx-infos)
            (au/<? (<send-msg {:msg-type :log-producer-tx-batch
                               :arg ser-tx-infos
                               :timeout-ms (* 1000 1000)}))
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
                              (if (or (empty? tx-ids)
                                      (= actor-id-str aid))
                                acc
                                (conj acc aid)))
                            [actor-id]
                            aid->tx-ids)]
        ;; Sync the whole log for the current actor-id, then sync
        ;; batches for any other actor-ids
        (doseq [aid aids]
          (au/<? (<sync-producer-txs-for-actor-id!
                  (assoc arg
                         :actor-id aid
                         :storage storage
                         :sync-whole-log? (= actor-id aid)
                         :unsynced-log-k unsynced-log-k))))))))

(defn <sync-consumer-txs!*
  [{:keys [*actor-id *client-running? *connected? *env-name *host-fns
           *state-info *storage root schema]
    :as arg}]
  ;; TODO: Don't re-apply own txns
  (au/go
    (au/<? (<wait-for-init arg))
    (when @*connected?
      (let [actor-id @*actor-id
            storage @*storage
            {:keys [<request-schema <send-msg update-subscriptions!]} @*host-fns
            msg-arg {:msg-type :get-consumer-sync-info
                     :arg @*state-info}
            sync-info (when @*connected? (au/<? (<send-msg msg-arg)))
            {:keys [snapshot-url
                    snapshot-tx-index
                    tx-ids-since-snapshot]} sync-info
            last-tx-index (when snapshot-tx-index
                            (+ snapshot-tx-index
                               (count tx-ids-since-snapshot)))
            snapshot (au/<? (common/<get-snapshot-from-url
                             (assoc arg
                                    :<request-schema <request-schema
                                    :url snapshot-url
                                    :schema schema
                                    :storage storage)))
            ser-tx-infos (when (and @*connected?
                                    (seq tx-ids-since-snapshot))
                           (au/<? (<send-msg
                                   {:msg-type :get-tx-infos
                                    :arg {:tx-ids tx-ids-since-snapshot}})))
            tx-infos (au/<? (common/<serializable-tx-infos->tx-infos
                             (assoc arg
                                    :<request-schema <request-schema
                                    :serializable-tx-infos ser-tx-infos
                                    :storage storage)))
            usi-ret (update-state-info! (u/sym-map *state-info last-tx-index
                                                   schema snapshot tx-infos))]
        (cond
          snapshot
          (update-subscriptions! {:updated-paths [[root]]})

          (seq tx-infos)
          (update-subscriptions! usi-ret))))))

;; TODO: Flesh this out, make sure it completes before any
;; other state updates are processed. Wait in the <init! process?
(defn load-local-data! [{:keys [*host-fns root signal-consumer-sync!] :as arg}]
  (ca/go
    (au/<? (<wait-for-init arg))
    #_(try
        (let [{:keys [update-subscriptions!]} @*host-fns
              updated-paths [[root]]]
          (update-subscriptions! (u/sym-map updated-paths))
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

(defn make-get-in-state [{:keys [*state-info schema]}]
  (fn get-in-state [{:keys [path root] :as gis-arg}]
    (common/get-value-info (assoc gis-arg
                                  :crdt (:crdt @*state-info)
                                  :path (u/chop-root path root)
                                  :norm-path [root]
                                  :schema schema))))

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
        *env-name (atom nil)
        *host-fns (atom {})
        *state-info (atom {})
        *storage (atom nil)
        client-id (u/compact-random-uuid)
        init! (fn [{:keys [<request-schema <send-msg
                           connected? env-name storage update-subscriptions!]}]
                (reset! *env-name env-name)
                (reset! *host-fns (u/sym-map <request-schema
                                             <send-msg
                                             connected?
                                             update-subscriptions!))
                (reset! *storage storage))
        sync-arg (u/sym-map *actor-id *client-running? *connected?
                            *host-fns *env-name *state-info *storage
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
        mus-arg (u/sym-map *actor-id *env-name *host-fns *state-info *storage
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
                                (reset! *state-info {})
                                (load-local-data! lld-arg)))
        stop! (fn []
                (reset! *client-running? false)
                (stop-producer-sync!)
                (stop-consumer-sync!))]
    (load-local-data! lld-arg)
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :<wait-for-sync <wait-for-sync
               :get-in-state (make-get-in-state (u/sym-map *state-info schema))
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
