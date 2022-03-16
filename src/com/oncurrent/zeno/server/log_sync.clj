(ns com.oncurrent.zeno.server.log-sync
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.crdt.common :as crdt-common]
   [com.oncurrent.zeno.log-storage :as log-storage]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.storage :as server-storage]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn get-sync-session-info
  [{:keys [*conn-id->sync-session-info conn-id]}]
  (let [info (some-> @*conn-id->sync-session-info
                     (get conn-id))]
    (or info
        (throw (ex-info (str "conn-id " conn-id
                             " has not set sync session info.")
                        (u/sym-map conn-id))))))

(defn get-actor-id
  [{:keys [*conn-id->auth-info conn-id]}]
  (some-> @*conn-id->auth-info
          (get conn-id)
          (:actor-id)))

(defn add-actor-id-and-sync-session-info [arg]
  (let [ss-info (get-sync-session-info arg)
        actor-id (get-actor-id arg)]
    (-> (merge arg ss-info)
        (assoc :actor-id actor-id))))

(defn <handle-get-log-range
  [{:keys [storage] :as arg}]
  (let [arg* (-> arg
                 (add-actor-id-and-sync-session-info)
                 (assoc :segment-position :head))
        segment-id-k (server-storage/get-log-segment-id-k arg*)]
    (log-storage/<get-log-range (assoc arg*
                                       :get-log-segment-k
                                       server-storage/get-log-segment-k

                                       :segment-id-k
                                       segment-id-k))))

(defn <handle-get-log-status [arg]
  (au/go
    (let [log-type (:arg arg)
          arg* (-> arg
                   (add-actor-id-and-sync-session-info)
                   (assoc :segment-position :tail)
                   (assoc :log-type log-type))
          segment-id-k (server-storage/get-log-segment-id-k arg*)
          last-tx-i (au/<? (log-storage/<get-last-log-tx-i
                            (assoc arg*
                                   :get-log-segment-k
                                   server-storage/get-log-segment-k

                                   :segment-id-k
                                   segment-id-k)))]
      (u/sym-map last-tx-i log-type))))

(defn <handle-get-tx-info [arg]
  (log-storage/<get-tx-info arg))

(defn handle-set-sync-session-info
  [{:keys [*conn-id->sync-session-info conn-id arg]}]
  (let [{:keys [client-id branch]} arg]
    (swap! *conn-id->sync-session-info
           assoc conn-id (u/sym-map branch client-id)))
  true)

(defn <apply-tx!
  [{:keys [*branch->crdt-store branch crdt-schema tx-info] :as arg}]
  (au/go
    (let [<request-schema (su/make-schema-requester arg)
          ops (au/<? (crdt-common/<serializable-crdt-ops->crdt-ops
                      (assoc arg
                             :<request-schema <request-schema
                             :ops (:crdt-ops tx-info))))
          update-infos (au/<?
                        (crdt-common/<serializable-update-infos->update-infos
                         (assoc arg
                                :<request-schema <request-schema
                                :update-infos (:update-infos tx-info))))]
      (swap! *branch->crdt-store
             (fn [m]
               (update m branch
                       #(apply-ops/apply-ops
                         (assoc arg
                                :crdt %
                                :schema crdt-schema
                                :ops ops))))))))

(defn <update-consumer-log!
  [{:keys [*connected-actor-id->conn-ids
           *consumer-actor-id->last-branch-log-tx-i
           actor-id
           branch-tx-i]
    :as arg}]
  (au/go
    (let [consumer-branch-tx-i (or (some-> @*consumer-actor-id->last-branch-log-tx-i
                                           (get actor-id))
                                   -1)]
      (when (< consumer-branch-tx-i branch-tx-i)
        (let [arg* (-> arg
                       (assoc :log-type :branch)
                       (assoc :segment-position :head))
              segment-id-k (server-storage/get-log-segment-id-k arg*)
              tx-ids (au/<? (log-storage/<get-log-range
                             (assoc arg*
                                    :get-log-segment-k
                                    server-storage/get-log-segment-k

                                    :segment-id-k
                                    segment-id-k

                                    :start-tx-i
                                    consumer-branch-tx-i

                                    :end-tx-i
                                    branch-tx-i)))
              _ (doseq [tx-id tx-ids]
                  (au/<? (log-storage/<write-tx-id-to-log!
                          (assoc arg*
                                 :actor-id actor-id
                                 :log-type :consumer
                                 :tx-id tx-id))))
              last-tx-i (au/<? (log-storage/<get-last-log-tx-i
                                (assoc arg
                                       :get-log-segment-k
                                       server-storage/get-log-segment-k

                                       :log-type
                                       :consumer

                                       :segment-id-k
                                       segment-id-k

                                       :segment-position
                                       :tail)))
              msg-arg (assoc arg
                             :arg {:last-tx-i last-tx-i
                                   :log-type :consumer}
                             :msg-type-name :publish-log-status)
              conn-ids (-> @*connected-actor-id->conn-ids
                           (get actor-id))]
          (swap! *consumer-actor-id->last-branch-log-tx-i
                 (fn [m]
                   (assoc m actor-id branch-tx-i)))
          (doseq [conn-id conn-ids]
            (t2s/<send-msg! (assoc  msg-arg :conn-id conn-id))))))))

(defn <update-consumer-logs!
  [{:keys [*connected-actor-id->conn-ids storage] :as arg}]
  (au/go
    (let [branch-tx-i (au/<? (log-storage/<get-last-log-tx-i
                              (assoc arg :log-type :branch)))]
      (doseq [actor-id (keys @*connected-actor-id->conn-ids)]
        (au/<? (<update-consumer-log!
                (assoc arg :actor-id actor-id :branch-tx-i branch-tx-i)))))
    true))

(defn <handle-publish-log-status
  [{:keys [storage] :as arg}]
  (au/go
    (let [log-type (-> arg :arg :log-type)
          client-tx-i (-> arg :arg :last-tx-i)
          arg* (-> arg
                   (add-actor-id-and-sync-session-info)
                   (assoc :segment-position :tail)
                   (assoc :log-type :producer))
          segment-id-k (server-storage/get-log-segment-id-k arg*)
          arg** (assoc arg*
                       :get-log-segment-k server-storage/get-log-segment-k
                       :segment-id-k segment-id-k)
          server-tx-i (au/<? (log-storage/<get-last-log-tx-i arg**))
          <request-schema (su/make-schema-requester arg)]
      (when (> client-tx-i server-tx-i)
        (let [range-info {:start-tx-i (if (neg? server-tx-i)
                                        0
                                        server-tx-i)
                          :end-tx-i client-tx-i
                          :log-type log-type}
              msg-arg (assoc arg
                             :arg range-info
                             :msg-type-name :get-log-range)
              tx-ids (au/<? (t2s/<send-msg! msg-arg))
              warg (-> arg**
                       (assoc :segment-position :tail)
                       (assoc :log-type :branch))
              segment-id-k (server-storage/get-log-segment-id-k warg)
              tail-segment-id-k (server-storage/get-log-segment-id-k warg)
              head-segment-id-k (server-storage/get-log-segment-id-k
                                 (assoc warg :segment-position :head))
              warg* (assoc warg
                           :get-log-segment-k server-storage/get-log-segment-k
                           :head-segment-id-k head-segment-id-k
                           :segment-id-k segment-id-k
                           :tail-segment-id-k tail-segment-id-k)
              pain (assoc arg**
                          :segment-id-k (server-storage/get-log-segment-id-k
                                         arg**)
                          :tail-segment-id-k (server-storage/get-log-segment-id-k
                                              arg**)
                          :head-segment-id-k (server-storage/get-log-segment-id-k
                                              (assoc arg**
                                                     :segment-position :head)))]
          (doseq [tx-id tx-ids]
            (when-not (au/<? (log-storage/<get-tx-info
                              (assoc arg** :tx-id tx-id)))
              (let [msg-arg (assoc arg
                                   :arg tx-id
                                   :msg-type-name :get-tx-info)
                    tx-info (au/<? (t2s/<send-msg! msg-arg))]
                (au/<? (log-storage/<write-tx-info!
                        (assoc arg** :tx-id tx-id :tx-info tx-info)))
                (au/<? (log-storage/<write-tx-id-to-log!
                        (assoc pain :tx-id tx-id)))
                (au/<? (log-storage/<write-tx-id-to-log!
                        (assoc warg* :tx-id tx-id)))
                (au/<? (<apply-tx! (assoc arg**
                                          :tx-id tx-id
                                          :tx-info tx-info))))))
          (au/<? (<update-consumer-logs! warg*)))))))
