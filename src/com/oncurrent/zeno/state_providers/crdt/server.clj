(ns com.oncurrent.zeno.state-providers.crdt.server
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(def branch-log-prefix "_BRANCH-LOG-")
(def branch-consumer-log-prefix "_BRANCH-CONSUMER-LOG-")

(defn branch->branch-log-k [branch]
  (when-not (string? branch)
    (throw (ex-info
            (str "`branch` in `branch->branch-log-k` must be a string. "
                 "Got `" (or branch "nil") "` of type `"
                 (or (type branch) "nil") "`.")
            (u/sym-map branch))))
  (str branch-log-prefix branch))

(defn info->branch-consumer-log-k [{:keys [actor-id branch]}]
  (str branch-consumer-log-prefix branch "-" actor-id))

(defn <store-tx-infos [{:keys [serializable-tx-infos storage]}]
  (au/go
    (doseq [{:keys [tx-id] :as serializable-tx-info} serializable-tx-infos]
      (let [k (common/tx-id->tx-info-k tx-id)]
        ;; Use <swap! instead of <add! so we can handle repeated idempotent
        ;; syncs. Consider properly comparing the byte arrays.
        (au/<? (storage/<swap! storage k shared/serializable-tx-info-schema
                               (constantly serializable-tx-info)))))
    true))

(defn make-<log-txs-handler
  [{:keys [*branch->crdt-store *storage root schema] :as fn-arg}]
  (fn [{:keys [<request-schema conn-id env-info] :as h-arg}]
    (au/go
      (let [branch (-> env-info :env-sp-root->info root
                       :state-provider-branch)
            branch-log-k (branch->branch-log-k branch)
            storage @*storage
            serializable-tx-infos (vec (:arg h-arg))
            _ (au/<? (<store-tx-infos (u/sym-map serializable-tx-infos
                                                 storage)))
            ;; Convert the serializable-tx-infos into regular tx-infos
            ;; so we can ensure that we have any required schemas
            ;; (which will be requested from the client).
            tx-infos (au/<? (common/<serializable-tx-infos->tx-infos
                             (u/sym-map <request-schema schema
                                        serializable-tx-infos storage)))
            {:keys [ops tx-ids]} (reduce (fn [acc {:keys [crdt-ops tx-id]}]
                                           (-> acc
                                               (update :ops concat crdt-ops)
                                               (update :tx-ids conj tx-id)))
                                         {:ops []
                                          :tx-ids []}
                                         tx-infos)]
        ;; TODO: Chop log into segments if it gets too long
        (au/<? (storage/<swap! storage branch-log-k
                               shared/segmented-log-schema
                               (fn [old-log]
                                 (update old-log :tx-ids concat tx-ids))))
        (swap! *branch->crdt-store update branch
               (fn [old-crdt]
                 (apply-ops/apply-ops {:crdt old-crdt
                                       :ops ops
                                       :schema schema})))
        true))))

(defn <get-serializable-txs-since
  [{:keys [branch last-tx-id storage]}]
  (au/go
    (loop [log-k (branch->branch-log-k branch)
           out []]
      (let [log (au/<? (storage/<get storage log-k
                                     shared/segmented-log-schema))
            {:keys [parent-log-k tx-ids]} log
            tx-ids-since (reduce
                          (fn [acc tx-id]
                            (if (= last-tx-id tx-id)
                              (reduced acc)
                              (conj acc tx-id)))
                          '()
                          (reverse tx-ids))
            tx-infos (au/<? (common/<get-serializable-tx-infos
                             {:storage storage
                              :tx-ids tx-ids-since}))
            new-out (concat out tx-infos)
            ;; If these are the same, we didn't find the last-tx-id yet
            more? (= (count tx-ids-since)
                     (count tx-ids))]
        (if (and more? parent-log-k)
          (recur parent-log-k new-out)
          new-out)))))

(defn make-<get-consumer-txs-handler [{:keys [*storage root]}]
  (fn [{:keys [arg env-info]}]
    ;; TODO: Implement authorization
    (let [branch (-> env-info :env-sp-root->info root :state-provider-branch)]
      (<get-serializable-txs-since {:branch branch
                                    :last-tx-id (:last-tx-id arg)
                                    :storage @*storage}))))

(defn make-<copy-branch! [{:keys [*branch->crdt-store *storage]}]
  (fn [{old-branch :state-provider-branch-source
        new-branch :state-provider-branch}]
    (au/go
      (let [storage @*storage
            old-log-k (branch->branch-log-k old-branch)
            new-log-k (branch->branch-log-k new-branch)
            new-log {:parent-log-k (when old-branch
                                     old-log-k)
                     :tx-ids []}]
        (storage/<swap! storage new-log-k shared/segmented-log-schema
                        (constantly new-log))
        (swap! *branch->crdt-store (fn [m]
                                     (assoc m new-branch
                                            (get m old-branch))))))))

(defn <load-branch! [{:keys [*branch->crdt-store branch schema storage]}]
  (au/go
   (let [<request-schema (fn [fp]
                           (throw
                            (ex-info
                             (str
                              "Schema for fingerprint `" fp "` not found while "
                              "loading state provider branch data. There is no "
                              "peer to request it from since the server is "
                              "just starting up. The storage that contains the "
                              "data should also contain the schema but it does "
                              "not. Perhaps there was or is a bug when writing "
                              "data to begin with.")
                             (-> (u/sym-map fp branch storage)
                                 (assoc :state-provider
                                        shared/state-provider-name)))))
         serializable-tx-infos (au/<? (-> (<get-serializable-txs-since
                                           {:branch branch
                                            :last-tx-id nil
                                            :storage storage})))
         tx-infos (au/<? (common/<serializable-tx-infos->tx-infos
                          (u/sym-map <request-schema schema
                                     serializable-tx-infos storage)))
         ops (reduce (fn [acc {:keys [crdt-ops tx-id update-infos]}]
                       (concat acc crdt-ops))
                     []
                     tx-infos)]
      (swap! *branch->crdt-store assoc branch
             (apply-ops/apply-ops {:crdt nil
                                   :ops ops
                                   :schema schema})))))

(defn <load-state! [{:keys [*branch->crdt-store *storage schema branches]}]
  (ca/go
    (try
      (let [storage @*storage]
        (doseq [branch branches]
          (au/<? (<load-branch!
                  (u/sym-map *branch->crdt-store branch schema storage)))))
      (catch Exception e
        (log/error (str "Exception in <load-state!:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn ->state-provider
  [{::crdt/keys [authorizer schema root]}]
  (when-not (keyword? root)
    (throw (ex-info (str "The `" ::crdt/root "` in the argument to "
                         "`->state-provider` must be a keyword. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map root))))
  (when-not (l/schema? schema)
    (throw (ex-info (str "The `" ::crdt/schema "` in the argument to "
                         "`->state-provider` must be a valid Lancaster schema. "
                         "Got: `" (or root "nil") "`.")
                    (u/sym-map schema))))
  (let [*<send-msg (atom nil)
        *branch->crdt-store (atom {})
        *storage (atom nil)
        <get-state (fn [{:zeno/keys [branch path]}]
                     (au/go
                       (common/get-value-info
                        {:crdt (get @*branch->crdt-store branch)
                         :path path
                         :norm-path []
                         :schema schema})))
        <update-state! (fn [{:zeno/keys [branch cmds] :as us-arg}]
                         (au/go
                           (swap! *branch->crdt-store
                                  update branch
                                  (fn [old-crdt]
                                    (let [pc-arg {:cmds cmds
                                                  :crdt old-crdt
                                                  :schema schema}]
                                      (-> (commands/process-cmds pc-arg)
                                          (:crdt)))))
                           true))
        arg (u/sym-map *branch->crdt-store *storage *<send-msg schema root)
        <copy-branch! (make-<copy-branch! arg)
        msg-handlers {:get-consumer-txs (make-<get-consumer-txs-handler arg)
                      :log-txs (make-<log-txs-handler arg)}
        init! (fn [{:keys [<send-msg branches storage] :as init-arg}]
                (reset! *storage storage)
                (reset! *<send-msg <send-msg)
                (<load-state! (assoc arg :branches branches)))]
    #::sp-impl{:<copy-branch! <copy-branch!
               :<get-state <get-state
               :<update-state! <update-state!
               :init! init!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :state-provider-name shared/state-provider-name}))
