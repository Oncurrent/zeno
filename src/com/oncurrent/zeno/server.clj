(ns com.oncurrent.zeno.server
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.distributed-mutex :as dm]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authenticator-impl :as auth-impl]
   [com.oncurrent.zeno.server.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def server-config-rules
  ;; TODO: Check that authenticators satisfy protocol
  ;; TODO: Check rpcs
  #:zeno{:admin-password
         {:required? true
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :authenticators
         {:required? true
          :checks [{:pred sequential?
                    :msg "must be a sequence of authenticators"}]}

         :certificate-str
         {:required? false
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :port
         {:required? true
          :checks [{:pred #(and (pos-int? %)
                                (<= % 65536))
                    :msg (str "must be a positive integer less than or "
                              "equal to 65536")}]}

         :private-key-str
         {:required? false
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :storage
         {:required? true
          :checks [{:pred #(satisfies? storage/IStorage %)
                    :msg "must satisfy the IStorage protocol"}]}

         :ws-url
         {:required? false
          :checks [{:pred string?
                    :msg "must be a string"}
                   {:pred #(or (str/starts-with? % "ws://")
                               (str/starts-with? % "wss://"))
                    :msg "must be start with `ws://` or `wss://`"}]}

         :<get-published-member-urls
         {:required? false
          :checks [{:pred fn?
                    :msg "must be a function that returns a channel"}]}

         :<publish-member-urls
         {:required? false
          :checks [{:pred fn?
                    :msg "must be a function that returns a channel"}]}})

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
    (let [<get-info #(storage/<get storage % schemas/cluster-member-info-schema)
          results-ch (->> member-ids
                          (map member-id->member-info-key)
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
                       member-ids)]
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
            (au/<? (<publish-member-urls cur-member-urls)))
          (ca/<! (ca/timeout (* 60 1000)))
          (when @*have-role?
            (recur (-> member-id->client
                       (add-member-clients storage added-member-ids)
                       (del-member-clients storage stale-member-ids)))))))))

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
                  on-acq (fn [_]
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
                  on-rel (fn [_]
                           (reset! *have-role? false))
                  opts (-> (select-keys role [:lease-length-ms])
                           (assoc :client-name ws-url)
                           (assoc :on-acquire on-acq)
                           (assoc :on-release on-rel))]
              (dm/make-distributed-mutex-client mutex-name storage opts)))
          roles)))

(defn <get-env-info [{:keys [env-name storage]}]
  (au/go
    (let [env-name->info (au/<? (storage/<get storage
                                              storage/env-name-to-info-key
                                              schemas/env-name-to-info-schema))]
      (get env-name->info env-name))))

(defn <do-rpc! [{:keys [*conn-id->auth-info
                        *conn-id->sync-session-info
                        *rpc-name-kw->handler
                        authenticator-name->info
                        conn-id
                        env-name
                        <get-state
                        <set-state!
                        <update-state!
                        rpcs
                        storage]
                 :as orig-arg}]
  ;; TODO: Add authorization
  (au/go
    (let [{:keys [arg rpc-id rpc-name-kw-name rpc-name-kw-ns]} (:arg orig-arg)
          rpc-name-kw (keyword rpc-name-kw-ns rpc-name-kw-name)]
      (try
        (let [handler (get @*rpc-name-kw->handler rpc-name-kw)
              <request-schema (su/make-schema-requester orig-arg)
              _ (when-not handler
                  (throw (ex-info (str "No handler found for RPC `"
                                       rpc-name-kw "`.")
                                  (u/sym-map rpc-name-kw arg))))
              {:keys [arg-schema ret-schema]} (get rpcs rpc-name-kw)
              deser-arg (au/<? (common/<serialized-value->value
                                {:<request-schema <request-schema
                                 :reader-schema arg-schema
                                 :serialized-value (-> orig-arg :arg :arg)
                                 :storage storage}))
              actor-id (some-> @*conn-id->auth-info
                               (get conn-id)
                               (:actor-id))
              h-arg #:zeno{:<get-state <get-state
                           :<set-state! <set-state!
                           :<update-state! <update-state!
                           :authenticator-name->info authenticator-name->info
                           :arg deser-arg
                           :actor-id actor-id
                           :conn-id conn-id
                           :env-name env-name}
              ret (handler h-arg)
              val (if (au/channel? ret)
                    (au/<? ret)
                    ret)]
          (au/<? (storage/<value->serialized-value storage ret-schema val)))
        (catch Exception e
          (log/error (str "RPC failed. rpc-name-kw: `" rpc-name-kw "` "
                          "rpc-id: `" rpc-id "`.\n\n"
                          (u/ex-msg-and-stacktrace e)))
          :zeno/rpc-error)))))

(defmulti update-state!* (fn [{:keys [cmds]}]
                           (reduce (fn [acc {:zeno/keys [path]}]
                                     (conj acc (first path)))
                                   #{}
                                   cmds)))

(defn make-root->state-provider-infos [env-info]
  (reduce (fn [acc {:keys [path-root
                           state-provider-name
                           state-provider-branch]}]
            (assoc acc path-root (u/sym-map state-provider-name
                                            state-provider-branch)))
          {}
          (:state-provider-infos env-info)))

(defn check-sp-spi-match [{:keys [env-name root sp spi]}]
  (let [{sp-get-name ::sp-impl/get-name} sp
        {spi-name :state-provider-name} spi
        sp-name (sp-get-name)]
    (when (and spi-name sp-name (not= sp-name spi-name))
      (throw (ex-info
              (str "State provider names do not match. The state provider "
                   "configured at server startup (via the "
                   "`root->state-provider` map) for this path root (`" root
                   "`)  is named `" (or sp-name "nil") "`. "
                   "The state provider named by the env `" (or env-name "nil")
                   "` is `" (or spi-name "nil") "`.")
              (u/sym-map env-name root sp-name spi-name))))))

(defn get-state-branch
  [{:keys [branch env-name root root->spis sp]}]
  (let [spi (root->spis root)]
    (check-sp-spi-match (u/sym-map env-name root sp spi))
    (or branch
        (:state-provider-branch spi)
        env-name)))

(defn check-get-state-arg [arg]
  ;; TODO: Implement
  )

(defn check-update-state-arg [{:zeno/keys [cmds]}]
  (when-not (sequential? cmds)
    (throw (ex-info
            (str "<update-state! must be called with a `:zeno/cmds` value that "
                 "is a sequence of commands. Got: `" (or cmds "nil") "`.")
            (u/sym-map cmds))))
  (let [roots (reduce (fn [acc {:zeno/keys [path]}]
                        (conj acc (first path)))
                      #{}
                      cmds)]
    (when-not (= 1 (count roots))
      (throw (ex-info
              (str "<update-state! cmds in a single call must all belong to "
                   "the same root. Got multiple roots: `" roots "`. Note that "
                   "this restriction may be lifted in the future.")
              (u/sym-map roots cmds))))))

;; TODO: Add more parameter checks to these fns
(defn <make-state-fns [{:keys [env-name root->state-provider] :as arg}]
  (au/go
    (let [env-info (au/<? (<get-env-info arg))
          root->spis (make-root->state-provider-infos env-info)
          <update-state! (fn [{:zeno/keys [branch cmds] :as us-arg}]
                           (check-update-state-arg us-arg)
                           (let [root (-> cmds first :zeno/path first)
                                 sp (root->state-provider root)
                                 branch* (get-state-branch
                                          (u/sym-map branch env-name root
                                                     root->spis sp))
                                 <us! (::sp-impl/<update-state! sp)]
                             (<us! (assoc us-arg :branch branch*))))]
      {:<get-state (fn [{:zeno/keys [branch path] :as gs-arg}]
                     (check-get-state-arg gs-arg)
                     (let [root (first path)
                           sp (root->state-provider root)
                           branch* (get-state-branch
                                    (u/sym-map branch env-name root
                                               root->spis sp))
                           <gs (::sp-impl/<get-state sp)]
                       (<gs (assoc gs-arg :branch branch*))))
       :<update-state! <update-state!
       :<set-state! (fn [{:zeno/keys [branch path value]}]
                      (<update-state!
                       #:zeno{:branch branch
                              :cmds [#:zeno{:arg value
                                            :op :zeno/set
                                            :path path}]}))})))

(defn <make-client-ep-info
  [{:keys [*conn-id->auth-info
           *conn-id->sync-session-info
           *connected-actor-id->conn-ids
           *rpc-name-kw->handler
           authenticator-name->info
           env-name
           rpcs
           storage]
    :as arg}]
  (when-not env-name
    (throw (ex-info "`:env-name` is nil" {})))
  (au/go
    (let [base-info (u/sym-map env-name storage)
          auth-info (u/sym-map *conn-id->auth-info
                               *connected-actor-id->conn-ids
                               authenticator-name->info)
          rpc-info (u/sym-map *rpc-name-kw->handler rpcs)
          state-fns (au/<? (<make-state-fns arg))]
      {:handlers {:get-authenticator-state
                  #(auth-impl/<handle-get-authenticator-state
                    (merge base-info auth-info state-fns %))

                  :get-schema-pcf-for-fingerprint
                  #(au/go
                     (-> (storage/<fp->schema storage (:arg %))
                         (au/<?)
                         (l/json)))

                  :log-in
                  #(auth-impl/<handle-log-in
                    (merge base-info auth-info state-fns %))

                  :log-out
                  #(auth-impl/<handle-log-out (merge base-info state-fns %))

                  :resume-login-session
                  #(auth-impl/<handle-resume-login-session
                    (merge base-info auth-info state-fns %))

                  :rpc
                  #(<do-rpc! (merge auth-info base-info rpc-info state-fns %))

                  :update-authenticator-state
                  #(auth-impl/<handle-update-authenticator-state
                    (merge base-info auth-info state-fns %))}
       :on-connect (fn [{:keys [conn-id remote-address] :as conn}]
                     (swap! *conn-id->auth-info assoc conn-id {})
                     (swap! *conn-id->sync-session-info assoc conn-id {})
                     (log/info
                      (str "Client connection opened:\n"
                           (u/pprint-str
                            (u/sym-map conn-id env-name remote-address)))))
       :on-disconnect (fn [{:keys [conn-id]}]
                        (when-let [actor-id (some-> @*conn-id->auth-info
                                                    (get conn-id)
                                                    (:actor-id))]
                          (swap! *connected-actor-id->conn-ids
                                 #(update % actor-id disj conn-id)))
                        (swap! *conn-id->auth-info dissoc conn-id)
                        (swap! *conn-id->sync-session-info dissoc conn-id)
                        (log/info
                         (str "Client connection closed:\n"
                              (u/pprint-str (u/sym-map conn-id env-name)))))
       :path (u/env-name->client-path-name env-name)
       :protocol schemas/client-server-protocol})))

(defn <handle-create-env [{:keys [server storage] :as arg}]
  (au/go
    (let [{:keys [env-name]} (:arg arg)
          ep-info (au/<? (<make-client-ep-info
                          (-> arg
                              (dissoc :arg)
                              (assoc :env-name env-name))))]
      (au/<? (storage/<swap! storage
                             storage/env-name-to-info-key
                             schemas/env-name-to-info-schema
                             (fn [env-name->info]
                               (when (contains? env-name->info env-name)
                                 (throw (ex-info (str "Env `" env-name "` "
                                                      "already exists.")
                                                 (u/sym-map env-name))))
                               (assoc env-name->info env-name (:arg arg)))))
      ;; TODO: Figure out how this works in a multi-server setup
      (t2s/add-endpoint! (assoc ep-info :server server))
      true)))

(defn <handle-delete-env [{:keys [server storage] :as arg}]
  (au/go
    (let [env-name (:arg arg)
          path (u/env-name->client-path-name env-name)]
      (au/<? (storage/<swap! storage
                             storage/env-name-to-info-key
                             schemas/env-name-to-info-schema
                             (fn [env-name->info]
                               (dissoc env-name->info env-name))))
      ;; TODO: Figure out how this works in a multi-server setup
      (t2s/remove-endpoint! (u/sym-map path server))
      true)))

(defn <handle-get-env-names [{:keys [server storage] :as arg}]
  (au/go
    (let [env-name->info (au/<? (storage/<get storage
                                              storage/env-name-to-info-key
                                              schemas/env-name-to-info-schema))]
      (or (keys env-name->info)
          []))))

(defn handle-admin-log-in
  [{:keys [*logged-in-admin-conn-ids admin-password arg conn-id]}]
  (if-not (= admin-password arg)
    false
    (do
      (swap! *logged-in-admin-conn-ids conj conn-id)
      true)))

(defn make-admin-client-ep-info [{:keys [*logged-in-admin-conn-ids] :as arg}]
  (let [make-handler (fn [f]
                       #(let [{:keys [conn-id]} %]
                          (if (@*logged-in-admin-conn-ids conn-id)
                            (f (merge % arg))
                            (throw (ex-info (str "Connection `" conn-id "` is "
                                                 "not logged in")
                                            %)))))]
    {:handlers {:create-env (make-handler <handle-create-env)
                :delete-env (make-handler <handle-delete-env)
                :get-env-names (make-handler <handle-get-env-names)
                :log-in #(handle-admin-log-in (merge % arg))}
     :on-connect (fn [{:keys [conn-id] :as conn}]
                   (log/info
                    (str "Admin client connection opened:\n"
                         (u/pprint-str
                          (select-keys conn [:conn-id :remote-address])))))
     :on-disconnect (fn [{:keys [conn-id]}]
                      (swap! *logged-in-admin-conn-ids disj conn-id)
                      (log/info
                       (str "Admin client connection closed:\n"
                            (u/pprint-str (u/sym-map conn-id)))))
     :path "/admin"
     :protocol schemas/admin-client-server-protocol}))

(defn <add-default-env [{:keys [server] :as arg}]
  (ca/go
    (try
      (let [arg* (assoc arg :env-name u/default-env-name)
            default-env-ep-info (au/<? (<make-client-ep-info arg*))]
        (t2s/add-endpoint! (assoc default-env-ep-info :server server)))
      (catch Exception e
        (log/error (str "Error adding default env:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn make-talk2-server [{:keys [port] :as arg}]
  (let [server (t2s/server (assoc arg :port port))
        admin-client-ep-info (make-admin-client-ep-info
                              (assoc arg :server server))]
    (<add-default-env (assoc arg :server server))
    (t2s/add-endpoint! (assoc admin-client-ep-info :server server))
    server))

(defn make-authenticator-storage [authenticator-name storage]
  (let [str-ns (namespace authenticator-name)
        str-name (name authenticator-name)
        prefix (if str-ns
                 (str str-ns "_" str-name)
                 str-name)]
    (storage/make-prefixed-storage prefix storage)))

(defn xf-authenticator-info [{:keys [authenticators storage]}]
  (reduce (fn [acc* authenticator]
            (let [authenticator-name (auth-impl/get-name
                                      authenticator)
                  storage-name (:storage-name authenticator)
                  authenticator-storage (make-authenticator-storage
                                         (if storage-name
                                           storage-name
                                           authenticator-name)
                                         storage)
                  info (u/sym-map authenticator
                                  authenticator-storage)]
              (assoc acc* authenticator-name info)))
          {}
          authenticators))

(defn zeno-server [config]
  (u/check-config {:config config
                   :config-type :server
                   :config-rules server-config-rules})
  (let [{:zeno/keys [<get-published-member-urls
                     <publish-member-urls
                     admin-password
                     authenticators
                     certificate-str
                     port
                     private-key-str
                     root->state-provider
                     rpcs
                     storage
                     ws-url]} config
        *conn-id->auth-info (atom {})
        *conn-id->sync-session-info (atom {})
        *connected-actor-id->conn-ids (atom {})
        *consumer-actor-id->last-branch-log-tx-i (atom {})
        *rpc-name-kw->handler (atom {})
        *logged-in-admin-conn-ids (atom #{})
        authenticator-name->info (xf-authenticator-info
                                  (u/sym-map authenticators storage))
        arg (u/sym-map *conn-id->auth-info
                       *conn-id->sync-session-info
                       *connected-actor-id->conn-ids
                       *consumer-actor-id->last-branch-log-tx-i
                       *logged-in-admin-conn-ids
                       *rpc-name-kw->handler
                       admin-password
                       authenticator-name->info
                       certificate-str
                       port
                       private-key-str
                       root->state-provider
                       rpcs
                       storage)
        talk2-server (make-talk2-server arg)
        member-id (u/compact-random-uuid)
        mutex-clients (when (and <get-published-member-urls
                                 <publish-member-urls)
                        (make-mutex-clients storage
                                            member-id
                                            ws-url
                                            <get-published-member-urls
                                            <publish-member-urls))]
    (u/sym-map *rpc-name-kw->handler
               member-id
               mutex-clients
               rpcs
               storage
               talk2-server
               ws-url)))

(defn stop! [zeno-server]
  (doseq [mutex-client (:mutex-clients zeno-server)]
    (dm/stop! mutex-client))
  (t2s/stop! (:talk2-server zeno-server)))

(defn set-rpc-handler! [zeno-server rpc-name-kw handler]
  (let [{:keys [*rpc-name-kw->handler]} zeno-server]
    (swap! *rpc-name-kw->handler assoc rpc-name-kw handler)))
