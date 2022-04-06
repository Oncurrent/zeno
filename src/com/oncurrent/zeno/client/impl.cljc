(ns com.oncurrent.zeno.client.impl
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.client :as aa]
   [com.oncurrent.zeno.client.authorization :as client-authz]
   [com.oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [com.oncurrent.zeno.client.client-commands :as client-commands]
   [com.oncurrent.zeno.client.storage :as client-storage]
   [com.oncurrent.zeno.crdt :as crdt]
   [com.oncurrent.zeno.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.crdt.commands :as crdt-commands]
   [com.oncurrent.zeno.crdt.common :as crdt-common]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.log-storage :as log-storage]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-config
  {:zeno/crdt-authorizer (aa/make-affirmative-authorizer)
   :zeno/crdt-branch "prod"
   :zeno/initial-client-state {}})

(def client-config-rules
  #:zeno{:crdt-authorizer
         {:required? false
          :checks [{:pred #(satisfies?
                            client-authz/IClientAuthorizer %)
                    :msg (str "must satisfy the IClientAuthorizer "
                              "protocol")}]}

         :crdt-branch
         {:required? false
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :crdt-schema
         {:required? false
          :checks [{:pred l/schema?
                    :msg "must be a valid Lancaster schema"}]}

         :get-server-url
         {:required? false
          :checks [{:pred ifn?
                    :msg "must be a function"}]}

         :initial-client-state
         {:required? false
          :checks [{:pred associative?
                    :msg "must be associative"}]}

         :storage
         {:required? false
          :checks [{:pred #(satisfies? storage/IStorage %)
                    :msg "must satisfy the IStorage protocol"}]}})

(def default-rpc-timeout-ms 30000)

(defn make-schema-requester [talk2-client]
  (fn [fp]
    (t2c/<send-msg! talk2-client :get-schema-pcf-for-fingerprint fp)))

(defn split-cmds [cmds]
  (reduce
   (fn [acc cmd]
     (common/check-cmd cmd)
     (let [{:zeno/keys [path]} cmd
           [head & tail] path]
       (update acc head #(conj (or % []) cmd))))
   {}
   cmds))

(defn <log-tx! [{:keys [zc ops update-infos] :as arg}]
  (au/go
    (let [{:keys [*actor-id client-name storage]} zc
          actor-id @*actor-id
          crdt-ops (au/<? (crdt-common/<crdt-ops->serializable-crdt-ops
                           (assoc zc :ops ops)))
          sys-time-ms (u/current-time-ms)
          update-infos (au/<?
                        (crdt-common/<update-infos->serializable-update-infos
                         (assoc zc :update-infos update-infos)))
          tx-info (u/sym-map actor-id crdt-ops sys-time-ms update-infos)
          tx-id (u/compact-random-uuid)
          arg* (-> zc
                   (assoc :segment-position :tail)
                   (assoc :log-type :producer))
          tail-segment-id-k (client-storage/get-log-segment-id-k arg*)
          head-segment-id-k (client-storage/get-log-segment-id-k
                             (assoc arg* :segment-position :head))]
      (au/<? (log-storage/<write-tx-info!
              (u/sym-map storage tx-id tx-info)))
      (au/<? (log-storage/<write-tx-id-to-log!
              {:get-log-segment-k client-storage/get-log-segment-k
               :head-segment-id-k head-segment-id-k
               :log-type :producer
               :storage storage
               :tail-segment-id-k tail-segment-id-k
               :tx-id tx-id})))))

(defn <apply-tx! [{:keys [*crdt-state crdt-schema tx-info talk2-client]
                   :as arg}]
  (au/go
    (let [<request-schema (make-schema-requester talk2-client)
          ops (au/<? (crdt-common/<serializable-crdt-ops->crdt-ops
                      (assoc arg
                             :<request-schema <request-schema
                             :ops (:crdt-ops tx-info))))
          update-infos (au/<?
                        (crdt-common/<serializable-update-infos->update-infos
                         (assoc arg
                                :<request-schema <request-schema
                                :update-infos (:update-infos tx-info))))]
      (swap! *crdt-state (fn [old-crdt]
                           (apply-ops/apply-ops
                            (assoc arg
                                   :crdt old-crdt
                                   :schema crdt-schema
                                   :ops ops))))
      (state-subscriptions/do-subscription-updates! arg update-infos))))

(defn <get-last-producer-log-tx-i [arg]
  (let [arg* (-> arg
                 (assoc :segment-position :tail)
                 (assoc :log-type :producer))
        segment-id-k (client-storage/get-log-segment-id-k arg*)]
    (log-storage/<get-last-log-tx-i
     (assoc arg*
            :get-log-segment-k client-storage/get-log-segment-k
            :segment-id-k segment-id-k))))

(defn <do-update-state! [zc cmds]
  ;; This is called serially from the update-state loop.
  ;; We can rely on there being no concurrent updates.
  ;; We need to execute all the commands transactionally. Either they
  ;; all commit or none commit. A transaction may include  many kinds of
  ;; updates.
  (au/go
    (let [{:keys [crdt-schema talk2-client *client-state *crdt-state]} zc
          root->cmds (split-cmds cmds)
          ;; When we support :zeno/online, do those cmds first and fail the txn
          ;; if the online cmds fail.
          crdt-ret (crdt-commands/process-cmds {:cmds (:zeno/crdt root->cmds)
                                                :crdt @*crdt-state
                                                :crdt-schema crdt-schema})
          ;; TODO: Log the crdt update infos and ops
          cur-client-state @*client-state
          client-ret (client-commands/eval-cmds cur-client-state
                                                (:zeno/client root->cmds)
                                                :zeno/client)
          update-infos (concat (:update-infos crdt-ret)
                               (:update-infos client-ret))]
      ;; We can use reset! here because we know this is called seriallly
      ;; with no concurrent updates.
      (reset! *client-state (:state client-ret))
      (reset! *crdt-state (:crdt crdt-ret))
      (when crdt-schema
        (au/<? (<log-tx! {:ops (:ops crdt-ret)
                          :update-infos (:update-infos crdt-ret)
                          :zc zc})))
      (state-subscriptions/do-subscription-updates! zc update-infos)
      (when (and crdt-schema talk2-client)
        (let [p-tx-i (au/<? (<get-last-producer-log-tx-i zc))]
          (au/<? (t2c/<send-msg! talk2-client
                                 :publish-log-status
                                 {:last-tx-i p-tx-i
                                  :log-type :producer}))))
      true)))

(defn start-update-state-loop! [zc]
  (ca/go
    (let [{:keys [*stop? update-state-ch]} zc]
      (loop []
        (try
          (let [[update-info ch] (ca/alts! [update-state-ch
                                            (ca/timeout 1000)])
                {:keys [cmds cb]} update-info]
            (try
              (when (= update-state-ch ch)
                (-> (<do-update-state! zc cmds)
                    (au/<?)
                    (cb)))
              (catch #?(:clj Exception :cljs js/Error) e
                (cb e))))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error "Error updating state:\n"
                       (u/ex-msg-and-stacktrace e))))
        (when-not @*stop?
          (recur))))))

(defn <sync-consumer-log! [{:keys [talk2-client] :as arg}]
  (au/go
    (let [server-ret (au/<? (t2c/<send-msg! talk2-client
                                            :get-log-status
                                            :consumer))
          server-consumer-tx-i (:last-tx-i server-ret)
          arg* (-> arg
                   (assoc :segment-position :tail)
                   (assoc :log-type :consumer))
          segment-id-k (client-storage/get-log-segment-id-k arg*)
          arg** (assoc arg*
                       :get-log-segment-k client-storage/get-log-segment-k
                       :segment-id-k segment-id-k)
          consumer-tx-i (au/<? (log-storage/<get-last-log-tx-i arg**))]
      (when (> server-consumer-tx-i consumer-tx-i)
        (let [range-info {:start-tx-i consumer-tx-i
                          :end-tx-i server-consumer-tx-i
                          :log-type :consumer}
              tx-ids (au/<? (t2c/<send-msg! talk2-client
                                            :get-log-range
                                            range-info))]
          (doseq [tx-id tx-ids]
            (when-not (au/<? (log-storage/<get-tx-info
                              (assoc arg :tx-id tx-id)))
              (let [tx-info (au/<? (t2c/<send-msg!
                                    talk2-client :get-tx-info tx-id))]
                (au/<? (log-storage/<write-tx-info!
                        (assoc arg :tx-id tx-id :tx-info tx-info)))
                (au/<? (log-storage/<write-tx-id-to-log!
                        (assoc arg** :tx-id tx-id)))
                (au/<? (<apply-tx! (assoc arg :tx-info tx-info)))))))))))

(defn <set-sync-session-info
  [{:keys [*client-id crdt-branch talk2-client] :as arg}]
  (au/go
    (loop []
      (if-let [client-id @*client-id]
        (au/<? (t2c/<send-msg! talk2-client :set-sync-session-info
                               {:branch crdt-branch
                                :client-id client-id}))
        (do
          (ca/<! (ca/timeout 50))
          (recur))))))

(defn start-sync-session-loop
  [{:keys [*actor-id *stop? end-sync-session-ch talk2-client] :as arg}]
  (ca/go
    (try
      (au/<? (<set-sync-session-info arg))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error in setting sync session info"
                   (u/ex-msg-and-stacktrace e))
        (ca/<! (ca/timeout 1000))))
    (loop []
      (try
        (let [p-tx-i (au/<? (<get-last-producer-log-tx-i arg))]
          (au/<? (t2c/<send-msg! talk2-client
                                 :publish-log-status
                                 {:last-tx-i p-tx-i
                                  :log-type :producer}))
          (au/<? (<sync-consumer-log! arg)))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in sync-session-loop"
                     (u/ex-msg-and-stacktrace e))
          (ca/<! (ca/timeout 1000))))
      (let [[v ch] (ca/alts! [end-sync-session-ch (ca/timeout 23000)])]
        (when (and (not= end-sync-session-ch ch)
                   (not @*stop?))
          (recur))))))

(defn make-on-connect
  [{:keys [*talk2-client start-sync-session storage update-state-ch] :as arg}]
  (fn [{:keys [protocol url]}]
    (ca/go
      (try
        (let [login-session-token (au/<? (storage/<get
                                          storage
                                          client-storage/login-session-token-key
                                          schemas/login-session-token-schema))]
          (if-not login-session-token
            (start-sync-session)
            (if (au/<? (t2c/<send-msg! @*talk2-client
                                       :resume-login-session
                                       login-session-token))
              (start-sync-session)
              (ca/put! update-state-ch
                       {:cb (fn [ret]
                              (start-sync-session))
                        :cmds [{:zeno/arg nil
                                :zeno/op :zeno/set
                                :zeno/path [:zeno/crdt]}]}))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in on-connect:\n"
                     (u/ex-msg-and-stacktrace e)))))))

(defn make-on-disconnect
  [{:keys [end-sync-session] :as arg}]
  (fn [{:keys [protocol url]}]
    (end-sync-session)))

(defn make-talk2-client [{:keys [*talk2-client get-server-url storage] :as arg}]
  (let [handlers {:get-log-range
                  (fn [{fn-arg :arg}]
                    (when-not (= :producer (:log-type fn-arg))
                      (throw (ex-info "Bad log-range request"
                                      (u/sym-map fn-arg))))
                    (let [arg* (-> (merge arg fn-arg)
                                   (assoc :segment-position :head))
                          segment-id-k (client-storage/get-log-segment-id-k
                                        arg*)]
                      (log-storage/<get-log-range
                       (assoc arg*
                              :get-log-segment-k client-storage/get-log-segment-k
                              :segment-id-k segment-id-k))))

                  :get-schema-pcf-for-fingerprint
                  (fn [{:keys [arg]}]
                    (au/go
                      (-> (storage/<fp->schema storage arg)
                          (au/<?)
                          (l/json))))

                  :get-tx-info
                  (fn [{tx-id :arg}]
                    (log-storage/<get-tx-info (assoc arg :tx-id tx-id)))

                  :publish-log-status
                  (fn [{fn-arg :arg}]
                    ;; TODO: Implement
                    )}]
    (t2c/client {:get-url get-server-url
                 :handlers handlers
                 :on-connect (make-on-connect arg)
                 :on-disconnect (make-on-disconnect arg)
                 :protocol schemas/client-server-protocol})))

(defn <get-client-id [storage]
  (au/go
    (or (au/<? (storage/<get storage
                             client-storage/client-id-key
                             schemas/client-id-schema))
        (let [client-id (u/compact-random-uuid)]
          (storage/<add! storage
                         client-storage/client-id-key
                         schemas/client-id-schema
                         client-id)
          client-id))))

(defn update-state! [zc cmds cb]
  ;; We put the updates on a channel to guarantee serial update order
  (ca/put! (:update-state-ch zc) (u/sym-map cmds cb)))

(defn zeno-client [config]
  (let [config* (merge default-config config)
        _ (u/check-config {:config config*
                           :config-type :client
                           :config-rules client-config-rules})
        {:zeno/keys [client-name
                     crdt-authorizer
                     crdt-branch
                     crdt-schema
                     get-server-url
                     initial-client-state
                     rpcs
                     storage]
         :or {storage (storage/make-storage
                       (storage/make-mem-raw-storage))}} config*
        *next-instance-num (atom 0)
        *next-topic-sub-id (atom 0)
        *topic-name->sub-id->cb (atom {})
        *stop? (atom false)
        *client-state (atom initial-client-state)
        ;; TODO: Load crdt-state from logs in IDB
        *crdt-state (atom nil)
        *state-sub-name->info (atom {})
        *actor-id (atom nil)
        *talk2-client (atom nil)
        *end-sync-session-ch (atom nil)
        end-sync-session #(when-let [ch @*end-sync-session-ch]
                            (ca/put! ch true))
        *client-id (atom nil)
        _ (ca/take! (<get-client-id storage) #(reset! *client-id %))
        update-state-ch (ca/chan (ca/sliding-buffer 1000))
        arg (u/sym-map *actor-id
                       *client-id
                       *stop?
                       client-name
                       crdt-authorizer
                       crdt-branch
                       crdt-schema
                       end-sync-session
                       get-server-url
                       rpcs
                       storage
                       update-state-ch)
        start-sync-session #(when (and crdt-branch crdt-schema)
                              (let [end-sync-session-ch (ca/promise-chan)
                                    talk2-client @*talk2-client]
                                (reset! *end-sync-session-ch
                                        end-sync-session-ch)
                                (start-sync-session-loop
                                 (assoc arg
                                        :end-sync-session-ch end-sync-session-ch
                                        :talk2-client talk2-client))))

        talk2-client (when (:zeno/get-server-url config*)
                       (let [arg* (assoc arg
                                         :*talk2-client *talk2-client
                                         :start-sync-session start-sync-session)]
                         (make-talk2-client arg*)))
        _ (reset! *talk2-client talk2-client)
        on-actor-id-change (fn [actor-id]
                             (ca/put! update-state-ch
                                      {:cb (fn [ret]
                                             (end-sync-session)
                                             (start-sync-session))
                                       :cmds [{:zeno/arg nil
                                               :zeno/op :zeno/set
                                               :zeno/path [:zeno/crdt]}]}))
        zc (merge arg (u/sym-map *client-state
                                 *crdt-state
                                 *next-instance-num
                                 *next-topic-sub-id
                                 *state-sub-name->info
                                 *topic-name->sub-id->cb
                                 on-actor-id-change
                                 talk2-client))]
    (start-update-state-loop! zc)
    zc))

(defn stop! [{:keys [*stop? talk2-client] :as zc}]
  (reset! *stop? true)
  (when talk2-client
    (t2c/stop! talk2-client)))

(defn subscribe-to-topic! [zc topic-name cb]
  (let [{:keys [*next-topic-sub-id *topic-name->sub-id->cb]} zc
        sub-id (swap! *next-topic-sub-id inc)
        unsub! (fn unsubscribe! []
                 (swap! *topic-name->sub-id->cb
                        (fn [old-topic-name->sub-id->cb]
                          (let [old-sub-id->cb (old-topic-name->sub-id->cb
                                                topic-name)
                                new-sub-id->cb (dissoc old-sub-id->cb sub-id)]
                            (if (seq new-sub-id->cb)
                              (assoc old-topic-name->sub-id->cb topic-name
                                     new-sub-id->cb)
                              (dissoc old-topic-name->sub-id->cb topic-name)))))
                 true)]
    (swap! *topic-name->sub-id->cb assoc-in [topic-name sub-id] cb)
    unsub!))

(defn publish-to-topic! [zc topic-name msg]
  (when-not (string? topic-name)
    (throw (ex-info (str "`topic-name` argument to `publish-to-topic!` must "
                         "be a string. Got `" (or topic-name "nil") "`.")
                    (u/sym-map topic-name msg))))
  (let [{:keys [*topic-name->sub-id->cb]} zc]
    (ca/go
      (try
        (doseq [[sub-id cb] (@*topic-name->sub-id->cb topic-name)]
          (cb msg))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log/error (str "Error while distributing messages:\n"
                          (u/ex-msg-and-stacktrace e)))))))
  nil)

(defn logged-in?
  [zc]
  (boolean @(:*actor-id zc)))

(defn <rpc! [{:keys [arg cb rpc-name-kw timeout-ms zc]
              :or {timeout-ms default-rpc-timeout-ms}}]
  (au/go
    (when-not (keyword? rpc-name-kw)
      (throw (ex-info (str "The `rpc-name-kw` parameter must be a keyword. "
                           "Got `" (or rpc-name-kw "nil") "`.")
                      (u/sym-map rpc-name-kw arg))))
    (let [{:keys [rpcs storage talk2-client]} zc
          {:keys [arg-schema ret-schema]} (get rpcs rpc-name-kw)
          s-arg (au/<? (storage/<value->serialized-value
                        storage arg-schema arg))
          rpc-arg {:rpc-name-kw-ns (namespace rpc-name-kw)
                   :rpc-name-kw-name (name rpc-name-kw)
                   :arg s-arg}
          sv (au/<? (t2c/<send-msg! talk2-client :rpc rpc-arg))
          <request-schema (make-schema-requester talk2-client)]
      (when sv
        (au/<? (common/<serialized-value->value
                {:<request-schema <request-schema
                 :reader-schema ret-schema
                 :serialized-value sv
                 :storage storage}))))))

(defn rpc! [{:keys [cb] :as arg}]
  (ca/go
    (try
      (let [ret (au/<? (<rpc! arg))]
        (when cb
          (cb ret)))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log/error (str "Exception in rpc!:\n" (u/ex-msg-and-stacktrace e)))
        (when cb
          (cb e))))))
