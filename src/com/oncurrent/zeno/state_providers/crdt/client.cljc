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
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def tx-info-prefix "TX-INFO-FOR-TX-ID-")

(defn tx-id->tx-info-k [tx-id]
  (str tx-info-prefix tx-id))

(defn make-<update-state!
  [{:keys [*actor-id *crdt-state *host-fns *storage new-tx-ch schema] :as arg}]
  (fn [{:zeno/keys [cmds] :keys [prefix]}]
    (au/go
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
            tx-info {:crdt-ops ser-ops
                     :sys-time-ms (u/current-time-ms)
                     :update-infos ser-update-infos}]
        (au/<? (storage/<add! storage k shared/tx-info-schema tx-info))
        ;; TODO: log the ops
        update-infos))))

(defn start-sync-session! [{:keys [*host-fns]}]
  (ca/go
    (try
      (let [{:keys [<send-msg]} @*host-fns
            ret (au/<? (<send-msg {:msg-type :add-nums
                                   :arg [2 3 42]}))]
        (log/info (str "XXXXXXXXX:\nGot ret: `" ret "`.")))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error in statrt-sync-session!:\n"
                   (u/ex-msg-and-stacktrace e))))))

(defn end-sync-session! [{:keys []}]
  )

(defn ->state-provider
  [{::crdt/keys [authorizer schema] :as sp-arg}]
  ;; TODO: Check args
  ;; TODO: Load initial state from IDB
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
        *storage (atom nil)
        init! (fn [{:keys [<send-msg connected? storage]}]
                (reset! *host-fns (u/sym-map <send-msg connected?))
                (reset! *storage storage))
        msg-handlers {:notify #(log/info
                                (str "NNNNNN:\nGot notification: " %))}
        new-tx-ch (ca/chan (ca/dropping-buffer 1))
        mus-arg (u/sym-map *actor-id *crdt-state *host-fns *storage
                           new-tx-ch schema)]
    #::sp-impl{:<update-state! (make-<update-state! mus-arg)
               :get-in-state get-in-state
               :get-state-atom (constantly *crdt-state)
               :init! init!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :on-actor-id-change (fn [actor-id]
                                     (reset! *actor-id actor-id))
               :on-connect (fn [url]
                             (start-sync-session! (u/sym-map *host-fns)))
               :on-disconnect (fn [code]
                                (end-sync-session! {}))
               :state-provider-name shared/state-provider-name}))


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

#_(defn <apply-tx! [{:keys [*crdt-state schema tx-info talk2-client]
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
                                       :schema schema
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
