(ns com.oncurrent.zeno.state-providers.crdt.server
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
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

(def s3-client (aws/client {:api :s3}))
(aws/validate-requests s3-client true)

(def branch-consumer-log-prefix "_BRANCH-CONSUMER-LOG-")
(def branch-log-prefix "_BRANCH-LOG-")
(def branch-snapshot-info-prefix "_BRANCH-SNAPSHOT-INFO-")

(defn branch->branch-log-k [branch]
  (when-not (string? branch)
    (throw (ex-info
            (str "`branch` in `branch->branch-log-k` must be a string. "
                 "Got `" (or branch "nil") "` of type `"
                 (or (type branch) "nil") "`.")
            (u/sym-map branch))))
  (str branch-log-prefix branch))

(defn ->branch-consumer-log-k [{:keys [branch actor-id log-version]}]
  (str branch-consumer-log-prefix branch "-" actor-id "-" log-version))

(defn ->branch-snapshot-info-k [{:keys [branch]}]
  (str branch-snapshot-info-prefix branch))

(defn <read-bytes-from-s3 [{:keys [bucket k]}]
  (au/go
    (let [s3-arg {:op :GetObject
                  :request {:Bucket bucket
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket k ret)))
        (let [ba (ba/byte-array (:ContentLength ret))]
          (.read (:Body ret) ba)
          ba)))))

(defn <write-bytes-to-s3 [{:keys [ba bucket k]}]
  (au/go
    (let [s3-arg {:op :PutObject
                  :request {:ACL "private"
                            :Body ba
                            :Bucket bucket
                            :CacheControl "max-age=31622400"
                            :ContentType "application/octet-stream"
                            :Key k}}
          ret (au/<? (aws-async/invoke s3-client s3-arg))]
      (if (:cognitect.anomalies/category ret)
        (throw (ex-info (str ret)
                        (u/sym-map s3-arg bucket k ret)))
        true))))

(defn <store-tx-infos [{:keys [serializable-tx-infos storage]}]
  (au/go
    (doseq [{:keys [tx-id] :as serializable-tx-info} serializable-tx-infos]
      (let [k (common/tx-id->tx-info-k tx-id)]
        ;; Use <swap! instead of <add! so we can handle repeated idempotent
        ;; syncs. Consider properly comparing the byte arrays.
        (au/<? (storage/<swap! storage k shared/serializable-tx-info-schema
                               (constantly serializable-tx-info)))))
    true))

(defn <load-snapshot!
  [{:keys [*branch->crdt-info branch s3-snapshot-bucket schema
           snapshot-info storage]}]
  (au/go
    (let [<request-schema (fn [fp]
                            (throw
                             (ex-info
                              (str
                               "Schema for fingerprint `" fp "` not found "
                               "while loading state provider snapshot data. "
                               "There is no peer to request it from.")
                              (-> (u/sym-map fp branch storage)
                                  (assoc :state-provider
                                         shared/state-provider-name)))))
          snapshot-ba (au/<? (<read-bytes-from-s3
                              {:bucket s3-snapshot-bucket
                               :k (:s3-key snapshot-info)}))
          ;; TODO: Use proper reader and writer schemas
          snapshot (l/deserialize-same shared/snapshot-schema snapshot-ba)
          v (l/deserialize-same schema (-> snapshot :serialized-value :bytes))
          crdt (-> snapshot :serialized-crdt edn/read-string)]
      (swap! *branch->crdt-info assoc branch (u/sym-map crdt v)))))

(defn <write-snapshot!
  [{:keys [branch crdt s3-snapshot-bucket schema storage v]}]
  (au/go
    ;; TODO: Use proper writer schemas & fp
    (let [sv (l/serialize schema v)
          snapshot {:serialized-crdt (pr-str crdt)
                    :serialized-value {:bytes sv}}
          snap-k (u/compact-random-uuid)
          snap-info-k (->branch-snapshot-info-k (u/sym-map branch))
          snap-info {:last-txi 0 ; TODO: Fix
                     :s3-key snap-k}]
      (au/<? (<write-bytes-to-s3 {:ba (l/serialize shared/snapshot-schema
                                                   snapshot)
                                  :bucket s3-snapshot-bucket
                                  :k snap-k}))
      (au/<? (storage/<swap! storage snap-info-k
                             shared/snapshot-info-schema
                             (fn [old-info]
                               snap-info))))))

(defn make-<log-txs-handler
  [{:keys [*branch->crdt-info *storage root s3-snapshot-bucket schema
           signal-distribute-to-consumers!]
    :as fn-arg}]
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
            m (reduce (fn [acc {:keys [crdt-ops tx-id update-infos]}]
                        (-> acc
                            (update :ops concat crdt-ops)
                            (update :tx-ids conj tx-id)
                            (update :update-infos conj update-infos)))
                      {:ops []
                       :tx-ids []
                       :update-infos []}
                      tx-infos)
            {:keys [ops tx-ids update-infos]} m
            ;; TODO: Chop log into segments if it gets too long
            _ (au/<? (storage/<swap! storage branch-log-k
                                     shared/segmented-log-schema
                                     (fn [old-log]
                                       (update old-log :tx-ids concat tx-ids))))
            _ (swap! *branch->crdt-info update branch
                     (fn [old-info]
                       (let [crdt (apply-ops/apply-ops {:crdt (:crdt old-info)
                                                        :ops ops
                                                        :schema schema})
                             v (common/update-v
                                (assoc (u/sym-map crdt root schema update-infos)
                                       :v (:v old-info)))]
                         (u/sym-map crdt v))))
            {:keys [crdt v]} (@*branch->crdt-info branch)]
        (au/<? (<write-snapshot! (u/sym-map branch crdt s3-snapshot-bucket
                                            schema storage v)))
        (signal-distribute-to-consumers!)
        true))))

(defn make-<get-consumer-sync-info-handler [{:keys [*storage root]}]
  (fn [{:keys [actor-id arg conn-id env-info]}]
    (au/go
      (let [consumer-last-tx-i (:last-tx-i arg)
            consumer-log-length (if consumer-last-tx-i
                                  (inc consumer-last-tx-i)
                                  0)
            branch (-> env-info :env-sp-root->info root :state-provider-branch)
            log-k (branch->branch-log-k branch)
            storage @*storage
            log (au/<? (storage/<get storage log-k shared/segmented-log-schema))
            log-length (count log)]
        (when (> log-length consumer-log-length)
          (let [snap-info-k (->branch-snapshot-info-k (u/sym-map branch))
                last-snapshot-info (au/<? (storage/<get
                                           storage snap-info-k
                                           shared/snapshot-info-schema))
                {:keys [last-tx-i]} last-snapshot-info
                tx-infos-since-snapshot (nthrest log (if last-tx-i
                                                       (inc last-tx-i)
                                                       0))]
            (u/sym-map last-snapshot-info tx-infos-since-snapshot)))))))

(defn make-<copy-branch! [{:keys [*branch->crdt-info *storage]}]
  (fn [{old-branch :state-provider-branch-source
        new-branch :state-provider-branch}]
    ;; TODO: Copy latest snapshot and snapshot info
    (au/go
      (let [storage @*storage
            old-log-k (branch->branch-log-k old-branch)
            new-log-k (branch->branch-log-k new-branch)
            new-log {:parent-log-k (when old-branch
                                     old-log-k)
                     :tx-ids []}]
        (storage/<swap! storage new-log-k shared/segmented-log-schema
                        (constantly new-log))
        (swap! *branch->crdt-info (fn [m]
                                    (assoc m new-branch
                                           (get m old-branch))))))))

(defn <load-branch!
  [{:keys [branch storage] :as arg}]
  (au/go
    (let [snap-info-k (->branch-snapshot-info-k (u/sym-map branch))
          snapshot-info (au/<? (storage/<get
                                storage snap-info-k
                                shared/snapshot-info-schema))]
      (when snapshot-info
        (au/<? (<load-snapshot! (assoc arg :snapshot-info snapshot-info)))))))

(defn <load-state!
  [{:keys [*branch->crdt-info *storage s3-snapshot-bucket schema branches]}]
  (ca/go
    (try
      (let [storage @*storage]
        (doseq [branch branches]
          (au/<? (<load-branch!
                  (u/sym-map *branch->crdt-info branch s3-snapshot-bucket
                             schema storage)))))
      (catch Exception e
        (log/error (str "Exception in <load-state!:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <distribute-to-consumers!
  [{:keys [*storage]}]
  (au/go
    ;; TODO: Implement
    ))

(defn make-<update-state!
  [{:keys [*branch->crdt-info root s3-snapshot-bucket schema]}]
  (fn [{:zeno/keys [branch cmds] :as us-arg}]
    (au/go
      (swap! *branch->crdt-info
             update branch
             (fn [old-info]
               (let [pc-arg {:cmds cmds
                             :root root
                             :crdt (:crdt old-info)
                             :schema schema}
                     {:keys [crdt update-infos]} (commands/process-cmds pc-arg)
                     v (common/update-v
                        (assoc (u/sym-map crdt root schema
                                          update-infos)
                               :v (:v old-info)))]
                 (u/sym-map crdt v))))
      (let [info (@*branch->crdt-info branch)]
        ;; TODO: Write to log and snapshot
        )
      true)))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn make-<get-state [{:keys [*branch->crdt-info root schema]}]
  (fn [{:zeno/keys [branch path] :as gis-arg}]
    (au/go
      (let [{:keys [v]} (@*branch->crdt-info branch)]
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
                 :value v}
                path)))))

(defn ->state-provider
  [{::crdt/keys [authorizer schema s3-snapshot-bucket root]}]
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
        *branch->crdt-info (atom {})
        *storage (atom nil)
        dist-arg (u/sym-map *storage)
        dist-ret (u/start-task-loop!
                  {:loop-delay-ms 10000
                   :loop-name "<distribute-to-consumers!"
                   :task-fn #(<distribute-to-consumers! dist-arg)})
        {stop-distribute-to-consumers! :stop!
         signal-distribute-to-consumers! :now!} dist-ret
        arg (u/sym-map *branch->crdt-info *storage *<send-msg root
                       s3-snapshot-bucket schema
                       signal-distribute-to-consumers!)
        <copy-branch! (make-<copy-branch! arg)
        msg-handlers {:get-consumer-sync-info
                      (make-<get-consumer-sync-info-handler arg)

                      :log-txs
                      (make-<log-txs-handler arg)}
        init! (fn [{:keys [<send-msg branches storage] :as init-arg}]
                (reset! *storage storage)
                (reset! *<send-msg <send-msg)
                (<load-state! (assoc arg :branches branches)))
        stop! (fn []
                (stop-distribute-to-consumers!))]
    #::sp-impl{:<copy-branch! <copy-branch!
               :<get-state (make-<get-state arg)
               :<update-state! (make-<update-state! arg)
               :init! init!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
