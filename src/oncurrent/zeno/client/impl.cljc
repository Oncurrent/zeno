(ns oncurrent.zeno.client.impl
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [oncurrent.zeno.client.client-commands :as commands]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-config
  {:initial-client-state {}
   :data-storage (storage/make-storage (storage/make-mem-raw-storage))
   :log-storage (storage/make-storage (storage/make-mem-raw-storage))})

(def config-rules
  {:data-storage {:required? true
                  :checks [{:pred #(satisfies? storage/IStorage %)
                            :msg "must satisfy the IStorage protocol"}]}
   :log-storage {:required? true
                 :checks [{:pred #(satisfies? storage/IStorage %)
                           :msg "must satisfy the IStorage protocol"}]}
   :sys-schema {:required? false
                :checks [{:pred l/schema?
                          :msg "must be a valid Lancaster schema"}]}})

(defn check-config [{:keys [config config-type config-rules]}]
  (doseq [[k info] config-rules]
    (let [{:keys [required? checks]} info
          v (get config k)]
      (when (and required? (not v))
        (throw
         (ex-info
          (str "`" k "` is required but is missing from the client config map.")
          (u/sym-map k config))))
      (doseq [{:keys [pred msg]} checks]
        (when (and v pred (not (pred v)))
          (throw
           (ex-info
            (str "The value of `" k "` in the client config map is invalid. It "
                 msg ". Got `" (or v "nil") "`.")
            (u/sym-map k v config))))))))

(defn split-cmds [cmds]
  (reduce
   (fn [acc cmd]
     (commands/check-cmd cmd)
     (let [{:keys [path]} cmd
           [head & tail] path
           k (case head
               :client :client-cmds
               :sys :sys-cmds
               (let [[head & tail] path
                     disp-head (or head "nil")]
                 (throw
                  (ex-info (str "Paths must begin with one of "
                                commands/valid-path-roots
                                ". Got: `" disp-head "` in path `" path "`.")
                           (u/sym-map path head)))))]
       (update acc k conj (assoc cmd :path path))))
   {:client-cmds []
    :sys-cmds []}
   cmds))

(defn <do-sys-updates! [zc sys-cmds]
  (au/go
    #_(log/info (str "##############################:\n"
                     (u/pprint-str
                      (u/sym-map sys-cmds))))
    ;; TODO: Return state & update-infos even if sys-cmds is empty / nil
    ))

(defn <do-update-state! [zc cmds]
  ;; This is called serially from the update-state loop.
  ;; We can rely on there being no concurrent updates.
  ;; We need to execute all the commands transactionally. Either they
  ;; all commit or none commit. A transaction may include both `:sys` and
  ;; `:client` updates.
  (au/go
    (let [{:keys [*client-state]} zc
          {:keys [client-cmds sys-cmds]} (split-cmds cmds)

          ;; Do sys updates first; only do the client updates if sys succeeds
          sys-ret (au/<? (<do-sys-updates! zc sys-cmds))
          client-ret (commands/eval-cmds @*client-state client-cmds :client)
          update-infos (concat (:update-infos sys-ret)
                               (:update-infos client-ret))]
      (swap! *client-state (fn [state]
                             (:state client-ret)))
      (state-subscriptions/do-subscription-updates! zc update-infos)
      true)))

(defn start-update-state-loop! [zc]
  (ca/go
    (let [{:keys [*shutdown? update-state-ch]} zc]
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
        (when-not @(:*shutdown? zc)
          (recur))))))

(defn zeno-client [config]
  (let [config* (merge default-config config)
        _ (check-config {:config config*
                         :config-type :client
                         :config-rules config-rules})
        {:keys [data-storage
                initial-client-state
                log-storage]} config*
        *next-instance-num (atom 0)
        *next-topic-sub-id (atom 0)
        *topic-name->sub-id->cb (atom {})
        *shutdown? (atom false)
        *client-state (atom initial-client-state)
        *state-sub-name->info (atom {})
        *subject-id (atom nil)
        update-state-ch (ca/chan (ca/sliding-buffer 1000))
        zc (u/sym-map *next-instance-num
                      *next-topic-sub-id
                      *shutdown?
                      *client-state
                      *state-sub-name->info
                      *subject-id
                      *topic-name->sub-id->cb
                      data-storage
                      log-storage
                      update-state-ch)]
    (start-update-state-loop! zc)
    zc))

(defn shutdown! [zc]
  (reset! (:*shutdown? zc) true))

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
                         "be a string. Got `" topic-name "`.")
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
