(ns com.oncurrent.zeno.client.impl
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authorization :as authz]
   [com.oncurrent.zeno.client.authorization :as client-authz]
   [com.oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [com.oncurrent.zeno.client.client-commands :as client-commands]
   [com.oncurrent.zeno.crdt :as crdt]
   [com.oncurrent.zeno.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.crdt.commands :as crdt-commands]
   [com.oncurrent.zeno.crdt.common :as crdt-common]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-config
  {:crdt-authorizer (client-authz/make-affirmative-authorizer)
   :crdt-branch "prod"
   :initial-client-state {}})

(def client-config-rules
  {:crdt-authorizer {:required? false
                     :checks [{:pred #(satisfies? authz/IClientAuthorizer %)
                               :msg (str "must satisfy the IClientAuthorizer "
                                         "protocol")}]}
   :crdt-branch {:required? false
                 :checks [{:pred string?
                           :msg "must be a string"}]}
   :crdt-schema {:required? false
                 :checks [{:pred l/schema?
                           :msg "must be a valid Lancaster schema"}]}
   :get-server-url {:required? false
                    :checks [{:pred ifn?
                              :msg "must be a function"}]}
   :initial-client-state {:required? false
                          :checks [{:pred associative?
                                    :msg "must be associative"}]}
   :storage {:required? false
             :checks [{:pred #(satisfies? storage/IStorage %)
                       :msg "must satisfy the IStorage protocol"}]}})

(def consumer-log-key "_CONSUMER-LOG")
(def tx-id->tx-info-key-prefix "TX-ID-TO-TX-INFO-")
(def producer-log-key "_PRODUCER-LOG")

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

(defn <log-tx! [{:keys [zc] :as arg}]
  (au/go
    (let [{:keys [*actor-id client-name storage]} zc
          actor-id @*actor-id
          crdt-ops (au/<? (crdt-common/<crdt-ops->serializable-crdt-ops
                           (assoc zc
                                  :ops (:ops arg))))
          sys-time-ms (u/current-time-ms)
          update-infos (au/<?
                        (crdt-common/<update-infos->serializable-update-infos
                         (assoc zc :update-infos (:update-infos arg))))
          tx-info (u/sym-map actor-id crdt-ops sys-time-ms update-infos)
          tx-id (u/compact-random-uuid)]
      (au/<? (storage/<add! storage
                            (str tx-id->tx-info-key-prefix tx-id)
                            schemas/tx-info-schema
                            tx-info))
      (au/<? (storage/<swap! storage
                             producer-log-key
                             schemas/tx-log-schema
                             (fn [old-log]
                               (conj (or old-log []) tx-id)))))))

(defn <apply-tx [{:keys [*crdt-state crdt-schema tx-info talk2-client] :as arg}]
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

(defn start-log-sync-loop! [zc]
  (ca/go
    (let [{:keys [*stop? client-name storage talk2-client]} zc]
      (when talk2-client
        (loop []
          (try
            (let [producer-log (au/<? (storage/<get storage
                                                    producer-log-key
                                                    schemas/tx-log-schema))
                  pub-ret (au/<? (t2c/<send-msg! talk2-client
                                                 :publish-producer-log-status
                                                 {:tx-i (count producer-log)}))
                  server-consumer-tx-i (au/<? (t2c/<send-msg!
                                               talk2-client
                                               :get-consumer-log-tx-i
                                               nil))
                  consumer-log (au/<? (storage/<get storage
                                                    consumer-log-key
                                                    schemas/tx-log-schema))
                  consumer-tx-i (count consumer-log)]
              (when (> server-consumer-tx-i consumer-tx-i)
                (let [index-range {:start-i consumer-tx-i
                                   :end-i server-consumer-tx-i}
                      new-log-entries (au/<? (t2c/<send-msg!
                                              talk2-client
                                              :get-consumer-log-range
                                              index-range))]
                  (doseq [tx-id new-log-entries]
                    ;; TODO: Parallelize this?
                    (when-not (au/<? (storage/<get
                                      storage
                                      (str tx-id->tx-info-key-prefix tx-id)
                                      schemas/tx-info-schema))
                      ;; TODO: Don't fetch tx-infos that we already have
                      (let [tx-info (au/<? (t2c/<send-msg!
                                            talk2-client
                                            :get-tx-info
                                            tx-id))]
                        ;; TODO: Make overwriting a tx-info work (idempotent)
                        (au/<? (storage/<add! storage
                                              (str tx-id->tx-info-key-prefix tx-id)
                                              schemas/tx-info-schema
                                              tx-info))
                        (au/<? (<apply-tx (assoc zc :tx-info tx-info))))))
                  (au/<? (storage/<swap! storage
                                         consumer-log-key
                                         schemas/tx-log-schema
                                         (fn [old-log]
                                           (concat old-log
                                                   new-log-entries)))))))
            (catch #?(:clj Exception :cljs js/Error) e
              (log/error "Error in log sync loop:\n"
                         (u/ex-msg-and-stacktrace e))))
          (when-not @*stop?
            ;; TODO: Consider this timeout value
            (ca/<! (ca/timeout 1000))
            (recur)))))))

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
      (state-subscriptions/do-subscription-updates! zc update-infos)
      (when talk2-client
        (au/<? (<log-tx! {:ops (:ops crdt-ret)
                          :update-infos (:update-infos crdt-ret)
                          :zc zc})))
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

(defn make-talk2-client [{:keys [get-server-url storage]}]
  (let [handlers {:get-producer-log-range
                  (fn [{:keys [arg]}]
                    (au/go
                      (let [plog (au/<? (storage/<get storage
                                                      producer-log-key
                                                      schemas/tx-log-schema))
                            {:keys [start-i end-i]} arg]
                        (subvec plog start-i end-i))))

                  :get-schema-pcf-for-fingerprint
                  (fn [{:keys [arg]}]
                    (au/go
                      (-> (storage/<fp->schema storage arg)
                          (au/<?)
                          (l/json))))

                  :get-tx-info
                  (fn [{:keys [arg]}]
                    (storage/<get storage
                                  (str tx-id->tx-info-key-prefix arg)
                                  schemas/tx-info-schema))

                  :rpc
                  ;; TODO: Implement
                  (constantly nil)}]
    (t2c/client {:get-url get-server-url
                 :handlers handlers
                 :protocol schemas/client-server-protocol})))

(defn zeno-client [config]
  (let [config* (merge default-config config)
        _ (u/check-config {:config config*
                           :config-type :client
                           :config-rules client-config-rules})
        {:keys [branch
                client-name
                crdt-schema
                initial-client-state
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
        talk2-client (when (:get-server-url config*)
                       (make-talk2-client (assoc config*
                                                 :storage storage)))
        update-state-ch (ca/chan (ca/sliding-buffer 1000))
        zc (u/sym-map *actor-id
                      *client-state
                      *crdt-state
                      *next-instance-num
                      *next-topic-sub-id
                      *stop?
                      *state-sub-name->info
                      *topic-name->sub-id->cb
                      branch
                      client-name
                      crdt-schema
                      storage
                      talk2-client
                      update-state-ch)]
    (start-log-sync-loop! zc)
    (start-update-state-loop! zc)
    zc))

(defn stop! [{:keys [*stop? talk2-client] :as zc}]
  (reset! *stop? true)
  (when talk2-client
    (t2c/stop! talk2-client)))

(defn update-state! [zc cmds cb]
  ;; We put the updates on a channel to guarantee serial update order
  (ca/put! (:update-state-ch zc) (u/sym-map cmds cb)))

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
  (boolean (:*actor-id zc)))
