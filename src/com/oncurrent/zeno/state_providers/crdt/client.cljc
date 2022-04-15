(ns com.oncurrent.zeno.state-providers.crdt.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-<update-state! [{:keys [*crdt-state schema]}]
  (fn [{:zeno/keys [cmds]}]
    (au/go
      (let [ret (commands/process-cmds {:cmds cmds
                                        :crdt @*crdt-state
                                        :crdt-schema schema})
            {:keys [crdt ops update-infos]} ret]
        ;; We can use `reset!` here b/c there are no concurrent updates
        (reset! *crdt-state crdt)
        ;; TODO: log the ops
        update-infos))))

(defn ->state-provider
  [{::crdt/keys [authorizer schema]}]
  ;; TODO: Check args
  ;; TODO: Load initial state from IDB
  (let [*crdt-state (atom nil)
        get-in-state (fn [{:keys [path] :as arg}]
                       (common/get-value-info (assoc arg
                                                     :crdt @*crdt-state
                                                     :path (rest path)
                                                     :schema schema)))]
    #::sp-impl{:<update-state! (make-<update-state!
                                (u/sym-map *crdt-state schema))
               :get-in-state get-in-state
               :get-name (constantly shared/state-provider-name)
               :get-state-atom (constantly *crdt-state)}))
#_#_
handlers  {:get-log-range
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

           :get-tx-info
           (fn [{tx-id :arg}]
             (log-storage/<get-tx-info (assoc arg :tx-id tx-id)))

           :publish-log-status
           (fn [{fn-arg :arg}]
             ;; TODO: Implement
             )}
;; start-sync-session #(let [end-sync-session-ch (ca/promise-chan)
;;                           talk2-client @*talk2-client]
;;                       (reset! *end-sync-session-ch
;;                               end-sync-session-ch)
;;                       (start-sync-session-loop
;;                        (assoc arg
;;                               :end-sync-session-ch end-sync-session-ch
;;                               :talk2-client talk2-client)))

;; *end-sync-session-ch (atom nil)
;; end-sync-session #(when-let [ch @*end-sync-session-ch]
;;                     (ca/put! ch true))

#_(defn start-sync-session-loop
    [{:keys [*actor-id *stop? end-sync-session-ch talk2-client] :as arg}]
    (ca/go
      #_(try
          (au/<? (<set-sync-session-info arg))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error "Error in setting sync session info"
                       (u/ex-msg-and-stacktrace e))
            (ca/<! (ca/timeout 1000))))
      #_(loop []
          (try
            (let [p-tx-i (au/<? (<get-last-producer-log-tx-i arg))]
              (au/<? (t2c/<send-msg! talk2-client
                                     :publish-log-status
                                     {:last-tx-i p-tx-i
                                      :log-type :producer}))
              #_(au/<? (<sync-consumer-log! arg)))
            (catch #?(:clj Exception :cljs js/Error) e
              (log/error "Error in sync-session-loop"
                         (u/ex-msg-and-stacktrace e))
              (ca/<! (ca/timeout 1000))))
          (let [[v ch] (ca/alts! [end-sync-session-ch (ca/timeout 23000)])]
            (when (and (not= end-sync-session-ch ch)
                       (not @*stop?))
              (recur))))))

#_(defn <set-sync-session-info
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

#_(defn <log-tx! [{:keys [zc ops update-infos] :as arg}]
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

#_(defn <apply-tx! [{:keys [*crdt-state crdt-schema tx-info talk2-client]
                     :as arg}]
    (au/go
      #_(let [<request-schema (make-schema-requester talk2-client)
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

#_(defn <get-last-producer-log-tx-i [arg]
    (let [arg* (-> arg
                   (assoc :segment-position :tail)
                   (assoc :log-type :producer))
          segment-id-k (client-storage/get-log-segment-id-k arg*)]
      (log-storage/<get-last-log-tx-i
       (assoc arg*
              :get-log-segment-k client-storage/get-log-segment-k
              :segment-id-k segment-id-k))))
