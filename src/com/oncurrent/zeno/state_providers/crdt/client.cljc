(ns com.oncurrent.zeno.state-providers.crdt.client
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.authorizer-impl :as authz-impl]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))


(def unsynced-log-prefix "_UNSYNCED-LOG-")

(defn ->unsynced-log-k [{:keys [env-name]}]
  (str unsynced-log-prefix env-name))

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

(defn <log-tx-infos!
  [{:keys [*actor-id *env-name *state-info *storage data-schema]}]
  (au/go
    #_
    (let [actor-id @*actor-id
          storage @*storage
          env-name @*env-name
          unsynced-log-k (->unsynced-log-k (u/sym-map env-name))
          {:keys [tx-infos-to-log]} @*state-info]
      (doseq [tx-info tx-infos-to-log]
        (let [{:keys [tx-id]} tx-info
              tx-info-k (common/tx-id->tx-info-k tx-id)
              ser-tx-info nil #_(au/<? (common/<tx-info->serializable-tx-info
                                        (u/sym-map data-schema storage tx-info)))]
          (au/<? (storage/<swap! storage
                                 tx-info-k
                                 shared/serializable-tx-info-schema
                                 (constantly ser-tx-info)))
          (au/<? (storage/<swap! storage unsynced-log-k
                                 shared/unsynced-log-schema
                                 (fn [aid->tx-ids]
                                   (update aid->tx-ids actor-id
                                           (fn [tx-ids]
                                             (conj (or tx-ids [])
                                                   tx-id))))))
          (swap! *state-info (fn [state-info]
                               (update state-info :tx-infos-to-log
                                       disj tx-info))))))))

(defn make-<update-state!
  [{:keys [*actor-id *state-info authorizer client-id make-tx-id data-schema
           signal-producer-sync!]
    :as mus-arg}]
  (fn [{:zeno/keys [cmds]
        :keys [root]
        :as fn-arg}]
    (au/go
      (au/<? (<wait-for-init mus-arg))
      (let [actor-id @*actor-id
            tx-info-base (common/make-update-state-tx-info-base
                          (assoc mus-arg
                                 :actor-id actor-id
                                 :cmds cmds))
            unauth-cmds (get-unauthorized-commands
                         (u/sym-map actor-id authorizer cmds))]
        (if (seq unauth-cmds)
          (throw (ex-info "Transaction aborted due to unauthorized cmds."
                          (u/sym-map cmds unauth-cmds actor-id)))

          (do
            (swap! *state-info
                   (fn [{:keys [tx-infos-to-log] :as state-info}]
                     (let [ret (commands/process-cmds {:cmds cmds
                                                       :crdt (:crdt state-info)
                                                       :data-schema data-schema
                                                       :root root})
                           {:keys [crdt crdt-ops]} ret
                           tx-info (assoc tx-info-base :crdt-ops crdt-ops)]
                       (assoc state-info
                              :crdt crdt
                              :tx-infos-to-log (conj (or tx-infos-to-log #{})
                                                     tx-info)))))
            (au/<? (<log-tx-infos! mus-arg))
            (signal-producer-sync!)
            (select-keys tx-info-base [:updated-paths])))))))

(defn remove-batch [{:keys [tx-ids batch]}]
  (let [batch-set (set batch)]
    (reduce (fn [acc tx-id]
              (if (batch-set tx-id)
                acc
                (conj acc tx-id)))
            []
            tx-ids)))

(defn <sync-producer-txs-for-actor-id!
  [{:keys [*client-running? *connected? *env-name *host-fns
           actor-id storage sync-whole-log? unsynced-log-k]}]
  (let [{:keys [<send-msg]} @*host-fns]
    (au/go
      #_
      (loop []
        (let [aid->tx-ids (au/<? (storage/<get storage
                                               unsynced-log-k
                                               shared/unsynced-log-schema))
              tx-ids (get aid->tx-ids actor-id)
              batch (set (take 10 tx-ids))
              ser-tx-infos (when (seq tx-ids)
                             nil #_(au/<? (common/<get-serializable-tx-infos
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
                                     (update aid->tx-ids actor-id
                                             (fn [tx-ids]
                                               (remove-batch
                                                (u/sym-map tx-ids batch))))))))
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
            storage @*storage
            unsynced-log-k (->unsynced-log-k {:env-name @*env-name})
            aid->tx-ids (au/<? (storage/<get storage
                                             unsynced-log-k
                                             shared/unsynced-log-schema))
            aids (reduce-kv (fn [acc aid tx-ids]
                              (if (or (empty? tx-ids)
                                      (= actor-id aid))
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
           *state-info *storage root]
    :as arg}]
  ;; TODO: Don't re-apply own txns
  (au/go
    #_
    (au/<? (<wait-for-init arg))
    #_(when @*connected?
        (let [actor-id @*actor-id
              storage @*storage
              {:keys [<request-schema <send-msg update-subscriptions!]} @*host-fns
              msg {:msg-type :get-consumer-sync-info
                   :arg (select-keys @*state-info [:last-tx-index])}
              sync-info (when @*connected? (au/<? (<send-msg msg)))
              {:keys [snapshot-url
                      snapshot-tx-index
                      tx-ids-since-snapshot]} sync-info
              last-tx-index (when snapshot-tx-index
                              (+ snapshot-tx-index
                                 (count tx-ids-since-snapshot)))
              snapshot nil #_(au/<? (common/<get-snapshot-from-url
                                     (assoc arg
                                            :<request-schema <request-schema
                                            :url snapshot-url
                                            :storage storage)))
              ser-tx-infos (when (and @*connected?
                                      (seq tx-ids-since-snapshot))
                             (au/<? (<send-msg
                                     {:msg-type :get-tx-infos
                                      :arg {:tx-ids tx-ids-since-snapshot}})))
              tx-infos nil #_(au/<? (common/<serializable-tx-infos->tx-infos
                                     (assoc arg
                                            :<request-schema <request-schema
                                            :serializable-tx-infos ser-tx-infos
                                            :storage storage)))
              tx-agg-info (reduce (fn [acc {:keys [crdt-ops updated-paths]}]
                                    (-> acc
                                        (update :crdt-ops
                                                set/union crdt-ops)
                                        (update :updated-paths
                                                concat updated-paths)))
                                  {:crdt-ops #{}
                                   :updated-paths []}
                                  tx-infos)
              {:keys [crdt-ops updated-paths]} tx-agg-info]
          (swap! *state-info
                 (fn [state-info]
                   ;; TODO: Handle case where this retries due to a concurrent
                   ;; update-state call and there was a snapshot.
                   (let [crdt (apply-ops/apply-ops
                               (assoc arg
                                      :crdt (:crdt (or snapshot state-info))
                                      :crdt-ops crdt-ops))]
                     (-> state-info
                         (assoc :crdt crdt)
                         (assoc :last-tx-index
                                (or last-tx-index
                                    (:last-tx-index state-info)))))))
          (cond
            snapshot
            (update-subscriptions! {:updated-paths [[root]]})

            (seq tx-infos)
            (update-subscriptions! (u/sym-map updated-paths)))))))

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

(defn make-get-in-state [{:keys [*state-info data-schema]}]
  (fn get-in-state [{:keys [path root] :as gis-arg}]
    (get/get-in-state (assoc gis-arg
                             :crdt (:crdt @*state-info)
                             :path path
                             :root root
                             :data-schema data-schema))))

(defn ->state-provider
  [{::crdt/keys [authorizer data-schema root] :as config}]
  ;; TODO: Check args
  ;; TODO: Load initial state from IDB
  (when-not authorizer
    (throw (ex-info (str "No authorizer was specified in the state provider "
                         "configuration.")
                    config)))

  (when-not data-schema
    (throw (ex-info (str "No `:data-schema` was specified in the state "
                         "provider configuration.")
                    config)))
  (let [*actor-id (atom u/unauthenticated-actor-id)
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
                            root data-schema)
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
                           authorizer client-id root data-schema
                           signal-producer-sync!)
        <wait-for-prod-sync (fn []
                              (au/go
                                (loop []
                                  (let [aid->tx-ids (au/<? (<get-unsynced-log
                                                            sync-arg))
                                        actor-id @*actor-id
                                        tx-ids (get aid->tx-ids actor-id)]
                                    (if (empty? tx-ids)
                                      true
                                      (do
                                        (ca/<! (ca/timeout 50))
                                        (recur)))))))
        <wait-for-cons-sync (fn []
                              (au/go
                                (loop []
                                  (let [msg {:msg-type :get-consumer-sync-info
                                             :arg (select-keys
                                                   @*state-info
                                                   [:last-tx-index])}
                                        connected? @*connected?
                                        {:keys [<send-msg]} @*host-fns
                                        sync-info (when connected?
                                                    (au/<? (<send-msg msg)))]
                                    (if (and connected? (empty? sync-info))
                                      true
                                      (do
                                        (ca/<! (ca/timeout 50))
                                        (recur)))))))
        <wait-for-sync (fn []
                         (au/go
                           (au/<? (<wait-for-init (u/sym-map *storage)))
                           (au/<? (<wait-for-prod-sync))
                           (au/<? (<wait-for-cons-sync))))
        lld-arg (assoc sync-arg :signal-consumer-sync! signal-consumer-sync!)
        <on-actor-id-change (fn [actor-id]
                              (au/go
                                (reset! *actor-id actor-id)
                                (reset! *state-info {})
                                (signal-producer-sync!)
                                ;; load-local-data! will signal consumer-sync
                                (load-local-data! lld-arg)))
        stop! (fn []
                (reset! *client-running? false)
                (stop-producer-sync!)
                (stop-consumer-sync!))]
    (load-local-data! lld-arg)
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :<wait-for-sync <wait-for-sync
               :get-in-state (make-get-in-state
                              (u/sym-map *state-info data-schema))
               :init! init!
               :msg-handlers {:notify-consumer-log-sync
                              (fn [arg]
                                (signal-consumer-sync!))}
               :msg-protocol shared/msg-protocol
               :<on-actor-id-change <on-actor-id-change
               :on-connect (fn [url]
                             (reset! *connected? true)
                             (signal-producer-sync!)
                             (signal-consumer-sync!))
               :on-disconnect (fn [code]
                                (reset! *connected? false))
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
