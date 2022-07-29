(ns com.oncurrent.zeno.state-providers.crdt.server
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [compact-uuids.core :as compact-uuid]
   [com.oncurrent.zeno.bulk-storage :as bulk-storage]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authorizer-impl :as server-authz]
   [com.oncurrent.zeno.server.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.state-providers.crdt.xf-schema :as xfs]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)
   (java.nio ByteBuffer)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(def branch-main-log-actor-id "__BRANCH_MAIN__")
(def branch-log-prefix "_BRANCH-LOG-")
(def snapshot-prefix "SNAPSHOT-")

(defn ->branch-log-k [{:keys [branch]}]
  (when-not (string? branch)
    (throw (ex-info
            (str "`:branch` in `->branch-log-k` arg must be a string. "
                 "Got `" (or branch "nil") "` of type `"
                 (or (type branch) "nil") "`.")
            (u/sym-map branch))))
  (str branch-log-prefix branch))

(defn ->snapshot-k [{:keys [snapshot-txs-hash]}]
  (when-not (int? snapshot-txs-hash)
    (throw (ex-info
            (str "`snapshot-txs-hash` in `->snapshot-k` must be a long. "
                 "Got `" (or snapshot-txs-hash "nil") "` of type `"
                 (or (type snapshot-txs-hash) "nil") "`.")
            (u/sym-map snapshot-txs-hash))))
  (str snapshot-prefix snapshot-txs-hash))

;; From https://en.wikipedia.org/wiki/Linear_congruential_generator
;; MMIX, Newlib, Musl
(def hash-multiplier 6364136223846793005)

(defn additive-hash
  "Returns a new additive hash value (a long), given an old additive
   hash value and a new value (a long) to add to the hash.
   This is a rolling hash without removal."
  [{:keys [old-hash new-v]}]
  (unchecked-add new-v (if old-hash
                         (unchecked-multiply old-hash hash-multiplier)
                         0)))

(defn pad-ba-8
  "Pads the end of the given byte array so the length is a multiple of 8"
  [ba]
  (let [bytes-to-add (- 8 (rem (count ba) 8))]
    (ba/concat-byte-arrays [ba (ba/byte-array bytes-to-add)])))

(defn add-tx-id-to-hash [{:keys [old-hash tx-id]}]
  ;; TODO: Use shift and mask instead of ByteBuffer
  (let [ba (-> (ba/utf8->byte-array tx-id)
               (pad-ba-8))
        num-longs (/ (count ba) 8)
        bb ^ByteBuffer (ByteBuffer/wrap ba)]
    (reduce (fn [acc i]
              (additive-hash {:new-v (.getLong bb)
                              :old-hash acc}))
            old-hash
            (range num-longs))))

(defn add-tx-ids-to-hash [{:keys [old-hash tx-ids]}]
  (reduce (fn [acc tx-id]
            (add-tx-id-to-hash {:old-hash acc
                                :tx-id tx-id}))
          old-hash
          tx-ids))

(defn <get-snapshot [{:keys [bulk-storage log-info] :as arg}]
  (au/go
    (let [snap-k (when (:snapshot-txs-hash log-info)
                   (->snapshot-k log-info))
          ba (au/<? (bulk-storage/<get bulk-storage snap-k))]
      (when ba
        (au/<? (c/<ba->snapshot (assoc arg :ba ba)))))))

(defn <get-crdt-ops-and-updated-paths-for-tx-ids [arg]
  (au/go
    (let [tx-infos (au/<? (c/<get-tx-infos arg))]
      (reduce (fn [acc {:keys [crdt-ops updated-paths]}]
                (-> acc
                    (update :crdt-ops set/union crdt-ops)
                    (update :updated-paths concat updated-paths)))
              {:crdt-ops #{}
               :updated-paths []}
              tx-infos))))

(defn <add-to-snapshot!
  [{:keys [bulk-storage log-info root crdt-schema data-schema storage tx-ids]
    :as arg}]
  (au/go
    (if (empty? tx-ids)
      (:snapshot-txs-hash log-info)
      (let [old-hash (:snapshot-txs-hash log-info)
            snapshot-txs-hash (add-tx-ids-to-hash (u/sym-map old-hash tx-ids))
            old-snapshot (au/<? (<get-snapshot arg))
            info (au/<? (<get-crdt-ops-and-updated-paths-for-tx-ids arg))
            crdt (apply-ops/apply-ops {:crdt (:crdt old-snapshot)
                                       :crdt-ops (:crdt-ops info)
                                       :data-schema data-schema
                                       :root root})
            sv {:bytes (l/serialize crdt-schema crdt)
                :fp (au/<? (storage/<schema->fp storage crdt-schema))}
            ba (l/serialize schemas/serialized-value-schema sv)
            snap-k (->snapshot-k (u/sym-map snapshot-txs-hash))]
        ;; TODO: Use S3 HeadObject instead of GetObject for this existence check
        (when-not (au/<? (bulk-storage/<get bulk-storage snap-k))
          (au/<? (bulk-storage/<put! bulk-storage snap-k ba)))
        snapshot-txs-hash))))

(defn make-new-snapshot!
  [{:keys [branch-tx-ids log-info tx-info] :as arg}]
  (let [{:keys [branch-log-tx-indices-since-snapshot
                snapshot-tx-index]} log-info
        {:keys [tx-id]} tx-info
        tx-ids (-> (mapv #(nth branch-tx-ids %)
                         branch-log-tx-indices-since-snapshot)
                   (conj tx-id))
        snapshot-txs-hash (au/<?? (<add-to-snapshot!
                                   (assoc arg :tx-ids tx-ids)))
        new-index (+ (or snapshot-tx-index -1)
                     (count tx-ids))]
    {:branch-log-tx-indices-since-snapshot []
     :snapshot-txs-hash snapshot-txs-hash
     :snapshot-tx-index new-index}))

(defn update-actor-log-info!
  [{:keys [i log-info snapshot-interval tx-info] :as arg}]
  (let [{:keys [tx-index]} tx-info
        {:keys [branch-log-tx-indices-since-snapshot]} log-info
        new-snapshot? (>= (inc (count branch-log-tx-indices-since-snapshot))
                          snapshot-interval)]
    (if new-snapshot?
      (make-new-snapshot! arg)
      (update log-info :branch-log-tx-indices-since-snapshot
              conj (or tx-index i)))))

(defn authorized? [{:keys [actor-id authorizer updated-paths]}]
  (reduce (fn [acc path]
            (if (server-authz/allowed? authorizer actor-id path nil
                                       ::server-authz/write)
              acc
              (reduced false)))
          true
          updated-paths))

(defn update-actor-id-to-log-info!
  [{:keys [actor-id-to-log-info tx-infos] :as arg}]
  (let [aitli (if (get actor-id-to-log-info branch-main-log-actor-id)
                actor-id-to-log-info
                (assoc actor-id-to-log-info branch-main-log-actor-id
                       {:branch-log-tx-indices-since-snapshot []
                        :snapshot-txs-hash 0
                        :snapshot-tx-index -1}))]
    (reduce
     (fn [acc i]
       (let [{:keys [updated-paths] :as tx-info} (nth tx-infos i)]
         (reduce-kv
          (fn [acc* actor-id log-info]
            (if-not (authorized? (assoc arg :updated-paths updated-paths))
              acc*
              (assoc acc* actor-id (update-actor-log-info!
                                    (assoc arg
                                           :i i
                                           :log-info log-info
                                           :tx-info tx-info)))))
          {}
          acc)))
     aitli
     (range (count tx-infos)))))

(defn update-branch-log-info!
  [{:keys [old-branch-log-info tx-infos] :as arg}]
  (let [{:keys [branch-tx-ids]} old-branch-log-info
        num-txs (count branch-tx-ids)
        tx-infos* (vec (map-indexed (fn [i tx-info]
                                      (assoc tx-info :tx-index (+ num-txs i)))
                                    tx-infos))
        tx-ids (map :tx-id tx-infos)
        branch-tx-ids* (vec (concat branch-tx-ids tx-ids))]
    (-> old-branch-log-info
        (assoc :branch-tx-ids branch-tx-ids*)
        (update :actor-id-to-log-info
                #(update-actor-id-to-log-info!
                  (assoc arg
                         :actor-id-to-log-info %
                         :branch-tx-ids branch-tx-ids*
                         :tx-infos tx-infos*))))))

(defn <add-to-branch-log! [{:keys [branch storage tx-infos] :as arg}]
  (au/go
    (let [branch-log-k (->branch-log-k (u/sym-map branch))]
      (au/<? (storage/<swap! storage branch-log-k
                             shared/branch-log-info-schema
                             #(update-branch-log-info!
                               (assoc arg
                                      :storage storage
                                      :old-branch-log-info %
                                      :tx-infos tx-infos)))))))

;; TODO: Write these in parallel?
(defn <store-tx-infos! [{:keys [tx-infos storage]}]
  (au/go
    (doseq [{:keys [tx-id] :as tx-info} tx-infos]
      (let [k (c/tx-id->tx-info-k tx-id)]
        ;; Use <swap! instead of <add! so we can handle repeated idempotent
        ;; syncs. Consider properly comparing the byte arrays.
        (au/<? (storage/<swap! storage k shared/tx-info-schema
                               (constantly tx-info)))))
    true))

(defn <log-producer-tx-batch!
  [{:keys [*branch->crdt-info <request-schema branch bulk-storage
           data-schema tx-infos storage]
    :as arg}]
  (au/go
    (au/<? (<store-tx-infos! (u/sym-map tx-infos storage)))
    (let [crdt-ops (reduce (fn [acc tx-info]
                             (set/union acc (:crdt-ops tx-info)))
                           #{}
                           tx-infos)]
      (swap! *branch->crdt-info update branch
             (fn [crdt-info]
               ;; TODO: Handle case where this retries due to a concurrent
               ;; update-state call and there was a snapshot.
               (let [crdt (apply-ops/apply-ops
                           (assoc arg
                                  :crdt (:crdt crdt-info)
                                  :crdt-ops crdt-ops))]
                 (-> crdt-info
                     (assoc :crdt crdt)))))
      (au/<? (<add-to-branch-log! (assoc arg :tx-infos tx-infos)))
      true)))

(defn make-log-producer-tx-batch-handler
  [{:keys [*bulk-storage *storage root] :as fn-arg}]
  (fn [{:keys [<request-schema env-info] :as h-arg}]
    (let [branch (-> env-info :env-sp-root->info root :state-provider-branch)]
      (<log-producer-tx-batch!
       (assoc fn-arg
              :<request-schema <request-schema
              :branch branch
              :bulk-storage @*bulk-storage
              :tx-infos (vec (:arg h-arg))
              :storage @*storage)))))

(defn make-get-tx-infos-handler
  ;; TODO: Consider security implications. Can anyone get any tx-info?
  [{:keys [*storage] :as fn-arg}]
  (fn [{:keys [arg] :as h-arg}]
    (c/<get-tx-infos (assoc fn-arg
                            :storage @*storage
                            :tx-ids (:tx-ids arg)))))

(defn <log-info->sync-info
  [{:keys [branch-tx-ids bulk-storage last-tx-index log-info] :as arg}]
  (au/go
    (let [{:keys [snapshot-txs-hash
                  branch-log-tx-indices-since-snapshot]} log-info
          snapshot-tx-index (or (:snapshot-tx-index log-info) -1)
          snapshot-k (when snapshot-txs-hash
                       (->snapshot-k log-info))
          seconds-valid (* 60 30)
          snapshot-url (when snapshot-txs-hash
                         (au/<? (bulk-storage/<get-time-limited-url
                                 bulk-storage snapshot-k seconds-valid)))
          log-tx-index (+ snapshot-tx-index
                          (count branch-log-tx-indices-since-snapshot))
          tx-ids-since-snapshot (mapv #(nth branch-tx-ids %)
                                      branch-log-tx-indices-since-snapshot)]
      (cond
        (nil? last-tx-index)
        (u/sym-map snapshot-url
                   snapshot-tx-index
                   tx-ids-since-snapshot)

        (= last-tx-index log-tx-index)
        {}

        (> last-tx-index snapshot-tx-index)
        (u/sym-map tx-ids-since-snapshot)

        :else
        (u/sym-map snapshot-url
                   snapshot-tx-index
                   tx-ids-since-snapshot)))))

(defn make-log-info-for-actor-id
  [{:keys [authorizer branch-tx-ids snapshot-interval]
    :as arg}]
  (let [tx-infos (au/<?? (c/<get-tx-infos
                          (assoc arg :tx-ids branch-tx-ids)))
        tx-indices (reduce
                    (fn [acc tx-index]
                      (let [tx-info (nth tx-infos tx-index)
                            {:keys [updated-paths]} tx-info]
                        (if (authorized? (assoc arg
                                                :updated-paths updated-paths))
                          (conj acc tx-index)
                          acc)))
                    []
                    (range (count branch-tx-ids)))
        tx-ids (map #(:tx-id (nth tx-infos %))
                    tx-indices)
        tail-len (mod (count tx-ids) snapshot-interval)
        split-i (- (count tx-ids) tail-len)
        snapshot-tx-index (when (pos? split-i)
                            (dec split-i))
        branch-log-tx-indices-since-snapshot (vec (drop split-i tx-indices))
        snapshot-tx-ids (take split-i tx-ids)
        snapshot-txs-hash (au/<?? (<add-to-snapshot!
                                   (assoc arg :tx-ids snapshot-tx-ids)))]
    (u/sym-map branch-log-tx-indices-since-snapshot
               snapshot-tx-index
               snapshot-txs-hash)))

(defn <get-consumer-sync-info
  [{:keys [actor-id branch last-tx-index storage] :as arg}]
  (au/go
    (let [branch-log-k (->branch-log-k (u/sym-map branch))
          bli (au/<? (storage/<get storage branch-log-k
                                   shared/branch-log-info-schema))
          {:keys [actor-id-to-log-info branch-tx-ids]} bli
          branch-tx-ids* (vec branch-tx-ids)
          ba-log-info (get actor-id-to-log-info actor-id)]
      (if ba-log-info
        (au/<? (<log-info->sync-info (assoc arg
                                            :branch-tx-ids branch-tx-ids*
                                            :last-tx-index last-tx-index
                                            :log-info ba-log-info)))
        (let [ba-log-info (make-log-info-for-actor-id
                           (assoc arg
                                  :actor-id actor-id
                                  :branch-tx-ids branch-tx-ids*))]
          (au/<? (storage/<swap!
                  storage branch-log-k shared/branch-log-info-schema
                  (fn [old]
                    (assoc-in old
                              [:actor-id-to-log-info actor-id]
                              ba-log-info))))
          (au/<? (<log-info->sync-info (assoc arg
                                              :branch-tx-ids branch-tx-ids*
                                              :last-tx-index last-tx-index
                                              :log-info ba-log-info))))))))

(defn make-get-consumer-sync-info-handler
  [{:keys [*bulk-storage *storage root] :as fn-arg}]
  (fn [{:keys [actor-id arg env-info] :as h-arg}]
    (let [branch (-> env-info :env-sp-root->info root :state-provider-branch)]
      (<get-consumer-sync-info (assoc fn-arg
                                      :actor-id actor-id
                                      :branch branch
                                      :bulk-storage @*bulk-storage
                                      :last-tx-index (:last-tx-index arg)
                                      :storage @*storage)))))

(defn make-<copy-branch! [{:keys [*branch->crdt-info *storage]}]
  (fn <copy-branch! [{new-branch :state-provider-branch
                      old-branch :state-provider-branch-source
                      :keys [temp?]
                      :as cb-arg}]
    (au/go
      ;; TODO: If temp? is true, store in temp-storage rather than storage

      (swap! *branch->crdt-info (fn [m]
                                  (assoc m new-branch (get m old-branch))))
      (let [storage @*storage
            old-branch-log-k (->branch-log-k {:branch old-branch})
            old-bli (au/<? (storage/<get storage old-branch-log-k
                                         shared/branch-log-info-schema))
            new-branch-log-k (->branch-log-k {:branch new-branch})]
        (when old-bli
          (au/<? (storage/<swap! storage new-branch-log-k
                                 shared/branch-log-info-schema
                                 (constantly old-bli))))
        true))))

(defn make-<delete-branch! [{:keys [*branch->crdt-info *storage]}]
  (fn <delete-branch! [{:keys [temp?]}]
    (au/go
      ;; TODO: Implement
      )))

(defn <load-branch!
  [{:keys [*branch->crdt-info *bulk-storage branch data-schema root storage]
    :as arg}]
  (au/go
    (let [branch-log-k (->branch-log-k (u/sym-map branch))
          bli (au/<? (storage/<get storage branch-log-k
                                   shared/branch-log-info-schema))
          {:keys [actor-id-to-log-info branch-tx-ids]} bli
          branch-tx-ids* (vec branch-tx-ids)
          log-info (get actor-id-to-log-info branch-main-log-actor-id)
          snapshot (au/<? (<get-snapshot
                           (assoc arg
                                  :bulk-storage @*bulk-storage
                                  :log-info log-info)))
          tx-ids (mapv #(nth branch-tx-ids* %)
                       (:branch-log-tx-indices-since-snapshot log-info))
          tx-infos (au/<? (c/<get-tx-infos (assoc arg :tx-ids tx-ids)))
          crdt-ops (reduce (fn [acc tx-info]
                             (set/union acc (:crdt-ops tx-info)))
                           #{}
                           tx-infos)
          crdt-info (apply-ops/apply-ops (assoc (u/sym-map crdt-ops data-schema)
                                                :crdt (:crdt snapshot)
                                                :root root))]
      (swap! *branch->crdt-info
             (fn [m]
               (assoc m branch crdt-info))))))

(defn <load-state!
  [{:keys [*branch->crdt-info *storage data-schema branches] :as arg}]
  (ca/go
    (try
      (let [storage @*storage]
        (doseq [branch branches]
          (au/<? (<load-branch! (assoc arg
                                       :branch branch
                                       :storage storage)))))
      (catch Exception e
        (log/error (str "Exception in <load-state!:\n"
                        (u/ex-msg-and-stacktrace e)))))))
(defn <log-tx-infos!
  [{:keys [*branch->crdt-info branch data-schema storage] :as arg}]
  (au/go
    (let [{:keys [tx-infos-to-log]} (get @*branch->crdt-info branch)]
      (doseq [tx-info tx-infos-to-log]
        (let [{:keys [tx-id]} tx-info
              tx-info-k (c/tx-id->tx-info-k tx-id)
              branch-log-k (->branch-log-k (u/sym-map branch))]
          (au/<? (storage/<swap! storage tx-info-k
                                 shared/tx-info-schema
                                 (constantly tx-info)))
          (au/<? (storage/<swap! storage branch-log-k
                                 shared/branch-log-info-schema
                                 #(update-branch-log-info!
                                   (assoc arg
                                          :old-branch-log-info %
                                          :tx-infos [tx-info]))))
          (swap! *branch->crdt-info update branch
                 (fn [info]
                   (update info :tx-infos-to-log disj tx-info))))))))

(defn make-<update-state!
  [{:keys [*branch->crdt-info *bulk-storage *storage
           data-schema make-tx-id root]
    :as mus-arg}]
  (fn [{:zeno/keys [actor-id branch cmds] :as us-arg}]
    (au/go
      (let [tx-info-base (c/make-update-state-tx-info-base
                          (assoc us-arg :make-tx-id make-tx-id))]
        (swap! *branch->crdt-info update branch
               (fn [{:keys [tx-infos-to-log] :as info}]
                 (let [ret (commands/process-cmds {:cmds cmds
                                                   :crdt (:crdt info)
                                                   :data-schema data-schema
                                                   :root root})
                       {:keys [crdt crdt-ops]} ret
                       tx-info (assoc tx-info-base :crdt-ops (seq crdt-ops))]
                   (assoc info
                          :crdt crdt
                          :tx-infos-to-log (conj (or tx-infos-to-log #{})
                                                 tx-info)))))
        (au/<? (<log-tx-infos! (assoc mus-arg
                                      :actor-id actor-id
                                      :branch branch
                                      :bulk-storage @*bulk-storage
                                      :storage @*storage)))
        true))))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn make-<get-state [{:keys [*branch->crdt-info root data-schema]}]
  (fn [{:zeno/keys [branch path] :as gs-arg}]
    (au/go
      (log/info (str "<<<<<<<gs:\n"
                     (u/pprint-str
                      (u/sym-map path root))))
      (get/get-in-state (assoc gs-arg
                               :crdt (some-> @*branch->crdt-info
                                             (get branch)
                                             :crdt)
                               :data-schema data-schema
                               :path path
                               :root root)))))

(defn ->state-provider
  [{::crdt/keys [authorizer data-schema s3-snapshot-bucket root]}]
  (when-not (keyword? root)
    (throw (ex-info (str "The `" ::crdt/root "` in the argument to "
                         "`->state-provider` must be a keyword. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "The `" ::crdt/data-schema "` in the argument to "
                         "`->state-provider` must be a valid Lancaster schema. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map data-schema))))
  (let [*<send-msg (atom nil)
        *branch->crdt-info (atom {})
        *storage (atom nil)
        *bulk-storage (atom nil)
        snapshot-interval 10
        crdt-schema (xfs/->crdt-schema (u/sym-map data-schema))
        arg (u/sym-map *branch->crdt-info *bulk-storage *storage
                       *<send-msg authorizer crdt-schema data-schema
                       root snapshot-interval)
        msg-handlers {:get-consumer-sync-info
                      (make-get-consumer-sync-info-handler arg)

                      :get-tx-infos
                      (make-get-tx-infos-handler arg)

                      :log-producer-tx-batch
                      (make-log-producer-tx-batch-handler arg)}
        init! (fn [{:keys [<send-msg branches bulk-storage storage]}]
                (reset! *storage storage)
                (reset! *bulk-storage bulk-storage)
                (reset! *<send-msg <send-msg)
                (<load-state! (assoc arg :branches branches)))
        stop! (fn []
                )]
    #::sp-impl{:<copy-branch! (make-<copy-branch! arg)
               :<delete-branch! (make-<delete-branch! arg)
               :<get-state (make-<get-state arg)
               :<update-state! (make-<update-state! arg)
               :init! init!
               :msg-handlers msg-handlers
               :msg-protocol shared/msg-protocol
               :state-provider-name shared/state-provider-name
               :stop! stop!}))
