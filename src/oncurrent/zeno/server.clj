(ns oncurrent.zeno.server
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.distributed-mutex :as dm]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-config
  {})

(def config-rules
  {:storage
   {:required? true
    :checks [{:pred #(satisfies? storage/IStorage %)
              :msg "must satisfy the IStorage protocol"}]}

   :ws-url
   {:required? true
    :checks [{:pred string?
              :msg "must be a string"}
             {:pred #(or (str/starts-with? % "ws://")
                         (str/starts-with? % "wss://"))
              :msg "must be start with `ws://` or `wss://`"}]}

   :<get-published-member-urls
   {:required? true
    :checks [{:pred fn?
              :msg "must be a function that returns a channel"}]}

   :<publish-member-urls
   {:required? true
    :checks [{:pred fn?
              :msg "must be a function that returns a channel"}]}})

(defn check-config [config]
  (doseq [[k info] config-rules]
    (let [{:keys [required? checks]} info
          v (get config k)]
      (when (and required? (not v))
        (throw
         (ex-info
          (str "`" k "` is required but is missing from the server config map.")
          (u/sym-map k config))))
      (doseq [{:keys [pred msg]} checks]
        (when (and pred (not (pred v)))
          (throw
           (ex-info
            (str "The value of `" k "` in the server config map is invalid. It "
                 msg ". Got `" v "`.")
            (u/sym-map k v config))))))))

(defn member-id->member-mutex-name [member-id]
  (str "role-cluster-member-" member-id))

(defn member-id->member-info-key [member-id]
  (str storage/member-info-key-prefix member-id))

(defn <store-member-info [storage member-id ws-url]
  (let [info (u/sym-map ws-url)
        k (member-id->member-info-key member-id)]
    (storage/<add! storage k schemas/cluster-member-info-schema info)))

(defn <get-member-info [storage member-id]
  (let [k (member-id->member-info-key member-id)]
    (storage/<get storage k schemas/cluster-member-info-schema)))

(defn <add-cluster-member! [storage member-id]
  (storage/<swap! storage
                  storage/cluster-membership-list-reference-key
                  schemas/cluster-membership-list-schema
                  (fn [old-list]
                    (-> (set old-list)
                        (conj member-id)))))

(defn <remove-cluster-member! [storage member-id]
  (au/<? (storage/<swap! storage
                         storage/cluster-membership-list-reference-key
                         schemas/cluster-membership-list-schema
                         (fn [old-list]
                           (remove #(= member-id %) old-list)))))

(defn make-member-watch-client [storage member-id]
  (let [mutex-name (member-id->member-mutex-name member-id)
        on-stale (fn [mutex-info]
                   (ca/go
                     (try
                       (au/<? (<remove-cluster-member! storage member-id))
                       (catch Exception e
                         (log/error (str "Error removing cluster member `"
                                         member-id "`.\n"
                                         (u/ex-msg-and-stacktrace e)))))))
        opts {:on-stale on-stale
              :watch-only? true}]
    (dm/make-distributed-mutex-client mutex-name storage opts)))

(defn add-member-clients [member-id->client storage added-member-ids]
  (reduce (fn [acc member-id]
            (let [client (make-member-watch-client storage member-id)]
              (assoc member-id->client member-id client)))
          member-id->client
          added-member-ids))

(defn del-member-clients [member-id->client storage stale-member-ids]
  (reduce (fn [acc member-id]
            (dm/stop! (member-id->client member-id))
            (dissoc member-id->client member-id))
          member-id->client
          stale-member-ids))

(defn <member-ids->member-urls [storage member-ids]
  (au/go
    (let [<get-info #(storage/<get storage % storage/cluster-member-info-schema)
          results-ch (->> member-info-keys
                          (map <get-info)
                          (ca/merge))
          num-ids (count member-ids)]
      (loop [urls []]
        (let [{:keys [ws-url]} (au/<? results-ch)
              new-urls (conj urls ws-url ws-url)]
          (if (< (count new-urls) num-ids)
            (recur new-urls)
            new-urls))))))

(defn <cluster-membership-manager-role [storage role-info *have-role?]
  (au/go
    (let [{:keys [<publish-member-urls <get-published-member-urls]} role-info
          member-ids (au/<? (storage/<get
                             storage
                             storage/cluster-membership-list-reference-key
                             schemas/cluster-membership-list-schema))
          clients (map #(make-member-watch-client storage %)
                       member-ids)])
    (loop [member-id->client (zipmap member-ids clients)]
      (let [prior-member-ids (keys member-id->client)
            cur-member-ids (-> (storage/<get
                                storage
                                storage/cluster-membership-list-reference-key
                                schemas/cluster-membership-list-schema)
                               (au/<?)
                               (set))
            added-member-ids (set/difference cur-member-ids prior-member-ids)
            stale-member-ids (set/difference prior-member-ids cur-member-ids)
            cur-member-urls (au/<? (<member-ids->member-urls storage
                                                             cur-member-ids))
            published-member-urls (-> (<get-published-member-urls)
                                      (au/<?)
                                      (set))]
        (when-not (= cur-member-urls published-member-urls)
          (au/<? (<publish-member-urls cur-member-urls))))
      (ca/<! (ca/timeout (* 60 1000)))
      (when @*have-role?
        (recur (-> member-id->client
                   (add-member-clients storage added-member-ids)
                   (del-member-clients storage stale-member-ids)))))))

(defn <member-health-role [storage role-info *have-role?]
  (au/go
    (let [{:keys [member-id ws-url]} role-info]
      (au/<? (<store-member-info storage member-id ws-url))
      (au/<? (<add-cluster-member! storage member-id)))))

(defn make-mutex-clients
  [storage member-id ws-url <get-published-member-urls <publish-member-urls]
  (let [roles [;; Role for the member to assert its membership and health
               {:mutex-name (member-id->member-mutex-name member-id)
                :lease-length-ms (* 60 1000)
                :role-fn <member-health-role
                :role-info (u/sym-map member-id ws-url)}

               {:mutex-name "role-cluster-membership-manager"
                :lease-length-ms (* 120 1000)
                :<role-fn <cluster-membership-manager-role
                :role-info (u/sym-map <get-published-member-urls
                                      <publish-member-urls)}]]
    (mapv (fn [role]
            (let [{:keys [<role-fn role-info mutex-name]} role
                  *have-role? (atom false)
                  on-acq (fn []
                           (reset! *have-role? true)
                           (when <role-fn
                             (ca/go
                               (try
                                 (au/<? (<role-fn storage
                                                  role-info
                                                  *have-role?))
                                 (catch Exception e
                                   (log/error
                                    (str "Error in <role-fn for mutex `"
                                         mutex-name "`:\n"
                                         (u/ex-msg-and-stacktrace e))))))))
                  on-rel #(reset! *have-role? false)
                  opts (-> (select-keys role [:lease-length-ms])
                           (assoc :client-name)
                           (assoc :on-acquire on-acq)
                           (assoc :on-release on-rel))]
              (dm/make-distributed-mutex-client mutex-name storage opts)))
          roles)))

(defn zeno-server [config]
  (let [config* (merge default-config config)
        _ (check-config config*)
        {:keys [<get-published-member-urls
                <publish-member-urls
                storage
                ws-url]} config*
        member-id (u/compact-random-uuid)
        mutex-clients (make-mutex-clients storage
                                          member-id
                                          ws-url
                                          <get-published-member-urls
                                          <publish-member-urls)
        zeno-server (u/sym-map member-id
                               mutex-clients
                               storage
                               ws-url)]
    zeno-server))

(defn stop! [zeno-server]
  (doseq [mutex-client (:mutex-clients zeno-server)]
    (dm/stop! mutex-client)))
