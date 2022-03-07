(ns com.oncurrent.zeno.server.log-sync
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.crdt.common :as crdt-common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def tx-id->tx-info-key-prefix "TX-ID-TO-TX-INFO-")

(defn make-producer-log-k [{:keys [client-id branch]}]
  (str "_" client-id "-" branch "-PRODUCER-LOG"))

;; TODO: Maybe serialize this per client?
(defn <handle-publish-producer-log-status
  [{:keys [storage conn-id] :as arg}]
  (au/go
    (let [client-tx-i (-> arg :arg :tx-i)
          ;; TODO: Get the real client-id
          client-id "test-client"
          ;; TODO: Get the real branch
          branch "test-branch"
          producer-log-k (make-producer-log-k (u/sym-map client-id branch))
          producer-log (au/<? (storage/<get storage
                                            producer-log-k
                                            schemas/tx-log-schema))
          server-tx-i (count producer-log)
          <request-schema (su/make-schema-requester arg)]
      (when (> client-tx-i server-tx-i)
        (let [index-range {:start-i server-tx-i
                           :end-i client-tx-i}
              msg-arg (assoc arg
                             :arg index-range
                             :msg-type-name :get-producer-log-range)
              new-log-entries (au/<? (t2s/<send-msg! msg-arg))]
          (doseq [tx-id new-log-entries]
            ;; TODO: Parallelize this?
            (when-not (au/<? (storage/<get
                              storage
                              (str tx-id->tx-info-key-prefix tx-id)
                              schemas/tx-info-schema))
              (let [tx-info (au/<? (t2s/<send-msg!
                                    (assoc arg
                                           :arg tx-id
                                           :msg-type-name :get-tx-info)))
                    ops (au/<? (crdt-common/<serializable-crdt-ops->crdt-ops
                                (assoc arg
                                       :<request-schema <request-schema
                                       :ops (:crdt-ops tx-info))))
                    update-infos (crdt-common/<serializable-update-infos->update-infos
                                  (assoc arg
                                         :<request-schema <request-schema
                                         :update-infos (:update-infos tx-info)))]
                ;; TODO: Make overwriting a tx-info work (idempotent)
                (au/<? (storage/<add! storage
                                      (str tx-id->tx-info-key-prefix tx-id)
                                      schemas/tx-info-schema
                                      tx-info)))))
          (au/<? (storage/<swap! storage
                                 producer-log-k
                                 schemas/tx-log-schema
                                 (fn [old-log]
                                   (concat old-log
                                           new-log-entries))))))
      :foobar)))

(defn <handle-get-consumer-log-range
  [{:keys [storage] :as arg}]
  (au/go
    (let [;; TODO: Get the real client-id
          client-id "test-client"
          ;; TODO: Get the real branch
          branch "test-branch"
          ;; TODO: Use authorization layer to copy from producer to consumer
          consumer-log-k (make-producer-log-k (u/sym-map client-id branch))
          consumer-log (au/<? (storage/<get storage
                                            consumer-log-k
                                            schemas/tx-log-schema))
          {:keys [start-i end-i]} (:arg arg)]
      (subvec consumer-log start-i end-i))))

(defn <handle-get-tx-info
  [{:keys [storage] :as arg}]
  (storage/<get storage
                (str tx-id->tx-info-key-prefix (:arg arg))
                schemas/tx-info-schema))

(defn <handle-get-consumer-log-tx-i
  [{:keys [storage] :as arg}]
  (au/go
    (let [;; TODO: Get the real client-id
          client-id "test-client"
          ;; TODO: Get the real branch
          branch "test-branch"
          ;; TODO: Use authorization layer to copy from producer to consumer
          consumer-log-k (make-producer-log-k (u/sym-map client-id branch))
          consumer-log (au/<? (storage/<get storage
                                            consumer-log-k
                                            schemas/tx-log-schema))]
      (count consumer-log))))
