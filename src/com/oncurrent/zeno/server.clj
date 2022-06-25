(ns com.oncurrent.zeno.server
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.bulk-storage :as bulk-storage]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authenticator-impl :as auth-impl]
   [com.oncurrent.zeno.server.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [lambdaisland.uri :as uri]
   [taoensso.timbre :as log]))

(def server-config-rules
  ;; TODO: Flesh this out
  #:zeno{:admin-password
         {:required? true
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :authenticators
         {:required? true
          :checks [{:pred sequential?
                    :msg "must be a sequence of authenticators"}]}

         :bulk-storage
         {:required? true
          :checks [{:pred #(satisfies? bulk-storage/IBulkStorage %)
                    :msg "must satisfy the IBulkStorage protocol"}]}

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

(defn <handle-rpc! [{:keys [*conn-id->auth-info
                            *rpc-name-kw->handler
                            conn-id
                            env-info
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
                  (let [known-handlers (keys @*rpc-name-kw->handler)]
                    (throw (ex-info (str "No handler found for RPC `"
                                         rpc-name-kw "`.")
                                    (u/sym-map rpc-name-kw
                                               known-handlers)))))
              {:keys [arg-schema ret-schema]} (get rpcs rpc-name-kw)
              deser-arg (au/<? (common/<serialized-value->value
                                {:<request-schema <request-schema
                                 :reader-schema arg-schema
                                 :serialized-value (-> orig-arg :arg :arg)
                                 :storage storage}))
              auth-info (some-> @*conn-id->auth-info
                                (get conn-id))
              actor-id (or (:actor-id auth-info)
                           u/unauthenticated-actor-id-str)
              h-arg #:zeno{:<get-state <get-state
                           :<set-state! <set-state!
                           :<update-state! <update-state!
                           :actor-id actor-id
                           :arg deser-arg
                           :authenticator-info auth-info
                           :conn-id conn-id
                           :env-info env-info
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

(defn xf-env-auth-info
  [authenticator-name->info
   env-name
   acc
   {:keys [authenticator-name authenticator-branch
           authenticator-branch-source] :as stored-info}]
  (let [branch (or authenticator-branch env-name)
        auth-info (authenticator-name->info authenticator-name)]
    (assoc acc authenticator-name
           (assoc auth-info
                  :authenticator-branch branch
                  :authenticator-branch-source authenticator-branch-source))))

(defn xf-perm-env-info
  [{:keys [authenticator-name->info env-info root->state-provider]}]
  (let [{:keys [env-name
                stored-authenticator-infos
                stored-state-provider-infos]} env-info
        env-auth-name->info (reduce
                             (partial xf-env-auth-info
                                      authenticator-name->info
                                      env-name)
                             {}
                             stored-authenticator-infos)
        env-sp-root->info (reduce
                           (fn [acc spi]
                             (let [{:keys [path-root
                                           state-provider-name
                                           state-provider-branch]} spi
                                   sp (root->state-provider path-root)
                                   branch (or state-provider-branch
                                              env-name)]
                               (assoc acc path-root
                                      {:state-provider sp
                                       :state-provider-branch branch})))
                           {}
                           stored-state-provider-infos)]
    {:env-authenticator-name->info env-auth-name->info
     :env-sp-root->info env-sp-root->info}))

(defn <delete-env-branches!
  [{:keys [env-info]}]
  (au/go
    (let [{:keys [env-authenticator-name->info env-sp-root->info]} env-info]
      (doseq [[auth-name auth-info] env-authenticator-name->info]
        (when (:authenticator-branch-source auth-info)
          (au/<? (auth-impl/<delete-branch! auth-info))))
      (doseq [[root sp-info] env-sp-root->info]
        (let [{:keys [state-provider]} sp-info
              {::sp-impl/keys [<delete-branch!]} state-provider]
          (when <delete-branch!
            (au/<? (<delete-branch! sp-info))))))))

(defn <copy-from-branch-sources!
  [{:keys [env-info source-env-name temp?]}]
  (au/go
    (let [{:keys [env-authenticator-name->info env-sp-root->info]} env-info]
      (doseq [[auth-name auth-info] env-authenticator-name->info]
        (let [source (or (:authenticator-branch-source auth-info)
                         source-env-name)]
          (when source
            (au/<? (auth-impl/<copy-branch!
                    (assoc auth-info
                           :authenticator-branch-source source
                           :temp? temp?))))))
      (doseq [[root sp-info] env-sp-root->info]
        (let [{:keys [state-provider]} sp-info
              source (or (:state-provider-branch-source sp-info)
                         source-env-name)
              {::sp-impl/keys [<copy-branch!]} state-provider]
          (when (and <copy-branch! source)
            (au/<? (<copy-branch! (assoc sp-info
                                         :state-provider-branch-source source
                                         :temp? temp?)))))))))

(defn <handle-create-env
  [{:keys [*env-name->info arg server storage] :as fn-arg}]
  (au/go
    (let [{:keys [env-name]} arg]
      (au/<? (storage/<swap! storage
                             storage/env-name-to-info-key
                             schemas/env-name-to-info-schema
                             (fn [env-name->info]
                               (when (contains? env-name->info env-name)
                                 (throw (ex-info (str "Env `" env-name "` "
                                                      "already exists.")
                                                 (u/sym-map env-name))))
                               (assoc env-name->info env-name arg))))
      (swap! *env-name->info assoc env-name
             (xf-perm-env-info (assoc fn-arg :env-info arg)))
      (au/<? (<copy-from-branch-sources!
              (assoc fn-arg
                     :env-info (get @*env-name->info env-name)))))
    true))

(defn <handle-delete-env
  [{:keys [*conn-id->env-name *env-name->info server storage talk2-server]
    :as arg}]
  (au/go
    (let [env-name (:arg arg)]
      (au/<? (storage/<swap! storage
                             storage/env-name-to-info-key
                             schemas/env-name-to-info-schema
                             (fn [env-name->info]
                               (dissoc env-name->info env-name))))
      (swap! *env-name->info dissoc env-name)
      (doseq [[conn-id env-name*] @*conn-id->env-name]
        (when (= env-name env-name*)
          (t2s/close-connection! {:conn-id conn-id
                                  :server talk2-server})))
      (au/<? (<delete-env-branches! {:env-info (@*env-name->info env-name)}))
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
    (when (> (count roots) 1)
      (throw (ex-info
              (str "<update-state! cmds in a single call must all belong to "
                   "the same root. Got multiple roots: `" roots "`. Note that "
                   "this restriction may be lifted in the future.")
              (u/sym-map roots cmds))))))

(defn check-get-state-arg [{:zeno/keys [path valid-path-roots]}]
  ;; TODO: Implement more?
  (let [[head tail] path]
      (when-not (valid-path-roots head)
        (throw (ex-info (str "Path must begin with one of " valid-path-roots
                             ". Got path `" path "`.")
                        (u/sym-map path head valid-path-roots))))))

(defn make-<update-state! [{:keys [env-name env-sp-root->info]}]
  (fn [{:zeno/keys [cmds] :as fn-arg}]
    (au/go
      (check-update-state-arg fn-arg)
      (let [root->cmds (reduce
                        (fn [acc {:zeno/keys [path] :as cmd}]
                          (let [[root & tail] path]
                            (update acc root conj (assoc cmd :zeno/path tail))))
                        {}
                        cmds)]
        (doseq [[root cmds] root->cmds]
          (let [sp-info (env-sp-root->info root)
                branch (or (:branch fn-arg)
                           (:state-provider-branch sp-info)
                           env-name)
                <us! (-> sp-info :state-provider ::sp-impl/<update-state!)]
            (au/<? (<us! (assoc fn-arg
                                :zeno/branch branch
                                :zeno/cmds cmds)))))
        true))))

(defn <ks-at-path [{:keys [full-path kw path root <get-state]}]
  (au/go
   (let [coll (:value (au/<? (<get-state (u/sym-map path root))))]
     (cond
      (map? coll)
      (keys coll)

      (sequential? coll)
      (range (count coll))

      (nil? coll)
      []

      :else
      (throw
       (ex-info
        (str "`" kw "` is in the path, but "
             "there is not a collection at " path ".")
        {:full-path full-path
         :missing-collection-path path
         :value coll}))))))

(defn <count-at-path [{:keys [path root <get-state]}]
  (au/go
   (let [coll (:value (au/<? (<get-state (u/sym-map path root))))]
     (cond
      (or (map? coll) (sequential? coll))
      (count coll)

      (nil? coll)
      0

      :else
      (throw
       (ex-info
        (str "`:zeno/count` terminates path, but there is not a collection at "
             path ".")
        {:path path
         :value coll}))))))

(defn <do-concat [{:keys [path root <get-state]}]
  (au/go
   (let [seqs (:value (au/<? (<get-state (u/sym-map path root))))]
     (when (and (not (nil? seqs))
                (or (not (sequential? seqs))
                    (not (sequential? (first seqs)))))
       (throw
        (ex-info
         (str "`:zeno/concat` terminates path, but there "
              "is not a sequence of sequences at " path ".")
         {:path path
          :value seqs})))
     (apply concat seqs))))

(defn <get-value-and-expanded-paths [{:keys [path root <get-state] :as fn-arg}]
  ;; TODO: Optimize this. Only traverse the path once.
  ;; TODO: Unify with what is in state_subscriptions.cljc?
  (au/go
   (let [last-path-k (last path)
         join? (u/has-join? path)
         wildcard-parent (-> (partition-by #(= :zeno/* %) path)
                             (first))
         wildcard? (not= path wildcard-parent)
         terminal-kw? (u/terminal-kw-ops last-path-k)
         <ks-at-path* #(<ks-at-path {:full-path path
                                     :kw :zeno/*
                                     :path %
                                     :root root
                                     :<get-state <get-state})]
     (cond
      (u/empty-sequence-in-path? path)
      [nil [path]]

      (and (not terminal-kw?) (not join?))
      (let [{:keys [norm-path value exists?]} (au/<? (<get-state
                                              (u/sym-map path root)))]
        [value [norm-path] exists?])

      (and terminal-kw? (not join?))
      (let [path* (butlast path)
            value (case last-path-k
                    :zeno/keys
                    (au/<? (<ks-at-path {:full-path path
                                         :kw :zeno/keys
                                         :path path*
                                         :root root
                                         :<get-state <get-state}))

                    :zeno/count
                    (au/<? (<count-at-path {:path path*
                                            :root root
                                            :<get-state <get-state}))

                    :zeno/concat
                    (au/<? (<do-concat {:path path*
                                        :root root
                                        :<get-state <get-state})))]
        [value [path*]])

      (and (not terminal-kw?) join?)
      (let [xpaths (au/<? (u/<expand-path <ks-at-path* path))
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          ;; Use loop to stay in go block
          (loop [out []
                 i 0]
            (let [path* (nth xpaths i)
                  [value _ exists?] (au/<? (<get-value-and-expanded-paths
                                            (assoc fn-arg :path path*)))
                  new-out (if (false? exists?) out (conj out value))
                  new-i (inc i)]
              (if (not= num-results new-i)
                (recur new-out new-i)
                [new-out xpaths*])))))

      (and terminal-kw? join?)
      (let [xpaths (au/<? (u/<expand-path <ks-at-path* (butlast path)))
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          (let [results (loop [out [] ;; Use loop to stay in go block
                               i 0]
                          (let [path* (nth xpaths i)
                                [value _ exists?] (au/<? (<get-value-and-expanded-paths
                                                          (assoc fn-arg :path path*)))
                                new-out (if (false? exists?) out (conj out value))
                                new-i (inc i)]
                            (if (not= num-results new-i)
                              (recur new-out new-i)
                              new-out)))
                v (case last-path-k
                    :zeno/keys (range (count results))
                    :zeno/count (count results)
                    :zeno/concat (apply concat results))]
            [v xpaths*])))))))

(defn make-<get-state [{:keys [env-name env-sp-root->info]}]
  (fn [{:zeno/keys [path] :as fn-arg}]
    (au/go
     (check-get-state-arg (assoc fn-arg :zeno/valid-path-roots
                                 (into #{} (keys env-sp-root->info))))
     (let [[root & tail] path
           sp-info (env-sp-root->info root)
           branch (or (:branch fn-arg)
                      (:state-provider-branch sp-info)
                      env-name)
           <get-state* (-> sp-info :state-provider ::sp-impl/<get-state)
           <get-state (fn [{p :path r :root}]
                        (<get-state* #:zeno{:branch branch :path p :root r}))]
       (first (au/<? (<get-value-and-expanded-paths
                      {:path tail :root root :<get-state <get-state})))))))

(defn make-state-fns [arg]
  (let [<update-state! (make-<update-state! arg)
        <get-state (make-<get-state arg)
        <set-state! (fn [{:zeno/keys [branch path value]}]
                      (<update-state! #:zeno{:branch branch
                                             :cmds [#:zeno{:arg value
                                                           :op :zeno/set
                                                           :path path}]}))]
    (u/sym-map <get-state <set-state! <update-state!)))

(defn <wait-for-connect [{:keys [*conn-id->connected? conn-id]}]
  (au/go
   (loop []
     (if (true? (get @*conn-id->connected? conn-id))
       true
       (do
        (ca/<! (ca/timeout 50))
        (recur))))))

(defn make-<auth-handler
  [{:keys [*conn-id->env-name *env-name->info <f] :as arg}]
  (let [base-info (select-keys arg [:*conn-id->auth-info
                                    :*connected-actor-id->conn-ids
                                    :storage])]
    (fn [{:keys [conn-id] :as handler-arg}]
      (au/go
        (au/<? (<wait-for-connect (assoc arg :conn-id conn-id)))
        (let [env-name (get @*conn-id->env-name conn-id)
              env-info (-> (get @*env-name->info env-name)
                           (assoc :env-name env-name))
              state-fns (make-state-fns env-info)]
          (au/<? (<f (merge base-info env-info state-fns handler-arg))))))))

(defn make-<rpc-handler
  [{:keys [*conn-id->env-name *env-name->info] :as arg}]
  (let [base-info (select-keys arg [:*conn-id->auth-info
                                    :*connected-actor-id->conn-ids
                                    :*rpc-name-kw->handler
                                    :rpcs
                                    :storage])]
    (fn [{:keys [conn-id] :as handler-arg}]
      (au/go
        (au/<? (<wait-for-connect (assoc arg :conn-id conn-id)))
        (let [env-name (get @*conn-id->env-name conn-id)
              env-info (-> (get @*env-name->info env-name)
                           (assoc :env-name env-name))
              state-fns (make-state-fns env-info)]
          (au/<? (<handle-rpc! (merge base-info
                                      state-fns
                                      handler-arg
                                      {:env-info env-info}))))))))

(defn make-<sp-rpc-handler
  [{:keys [*conn-id->auth-info *conn-id->env-name *env-name->info
           name->state-provider storage]
    :as make-handler-arg}]
  (fn [{:keys [conn-id] :as handler-arg}]
    (au/go
      (au/<? (<wait-for-connect (assoc make-handler-arg :conn-id conn-id)))
      (let [{:keys [arg rpc-id rpc-name-kw-name
                    rpc-name-kw-ns state-provider-name]} (:arg handler-arg)
            <request-schema (su/make-schema-requester handler-arg)
            rpc-name-kw (keyword rpc-name-kw-ns rpc-name-kw-name)]
        (try
          (let [sp (name->state-provider state-provider-name)
                {::sp-impl/keys [msg-handlers msg-protocol]} sp
                handler (get msg-handlers rpc-name-kw)
                _ (when-not handler
                    (throw (ex-info
                            (str "No handler found for in state provider `"
                                 state-provider-name "` for RPC `"
                                 rpc-name-kw "`.")
                            (u/sym-map rpc-name-kw state-provider-name))))
                env-name (get @*conn-id->env-name conn-id)
                env-info (-> (get @*env-name->info env-name)
                             (assoc :env-name env-name))
                auth-info (some-> @*conn-id->auth-info
                                  (get conn-id))
                actor-id (or (:actor-id auth-info)
                             u/unauthenticated-actor-id-str)
                {:keys [arg-schema ret-schema]} (get msg-protocol rpc-name-kw)
                deser-arg (au/<? (common/<serialized-value->value
                                  {:<request-schema <request-schema
                                   :reader-schema arg-schema
                                   :serialized-value arg
                                   :storage storage}))
                h-arg {:<request-schema <request-schema
                       :actor-id actor-id
                       :arg deser-arg
                       :conn-id conn-id
                       :env-info env-info}
                ret (handler h-arg)
                val (if (au/channel? ret)
                      (au/<? ret)
                      ret)]
            (au/<? (storage/<value->serialized-value storage ret-schema val)))
          (catch Exception e
            (log/error (str "RPC failed for state provider `"
                            state-provider-name
                            "`. rpc-name-kw: `" rpc-name-kw "` "
                            "rpc-id: `" rpc-id "`.\n\n"
                            (u/ex-msg-and-stacktrace e)))
            :zeno/rpc-error))))))

(defn make-<sp-msg-handler
  [{:keys [*conn-id->env-name *env-name->info name->state-provider storage]
    :as make-handler-arg}]
  (let [<request-schema (su/make-schema-requester make-handler-arg)]
    (fn [{:keys [conn-id] :as handler-arg}]
      (au/go
        (au/<? (<wait-for-connect (assoc make-handler-arg :conn-id conn-id)))
        (let [{:keys [arg msg-type-name
                      msg-type-ns state-provider-name]} (:arg handler-arg)
              msg-type (keyword msg-type-name msg-type-ns)]
          (try
            (let [sp (name->state-provider state-provider-name)
                  {::sp-impl/keys [msg-handlers msg-protocol]} sp
                  handler (get msg-handlers msg-type)
                  _ (when-not handler
                      (throw (ex-info
                              (str "No handler found for in state provider `"
                                   state-provider-name "` for msg `"
                                   msg-type"`.")
                              (u/sym-map msg-type state-provider-name))))
                  env-name (get @*conn-id->env-name conn-id)
                  env-info (-> (get @*env-name->info env-name)
                               (assoc :env-name env-name))
                  {:keys [arg-schema]} (get msg-protocol msg-type)
                  deser-arg (au/<? (common/<serialized-value->value
                                    {:<request-schema <request-schema
                                     :reader-schema arg-schema
                                     :serialized-value arg
                                     :storage storage}))
                  h-arg {:<request-schema <request-schema
                         :arg deser-arg
                         :conn-id conn-id
                         :env-info env-info}
                  ret (handler h-arg)]
              (when (au/channel? ret)
                ;; Check for exceptions
                (au/<? ret)))
            (catch Exception e
              (log/error (str "Msg handling failed for state provider `"
                              state-provider-name
                              "`. msg-type: `" msg-type "`.\n\n"
                              (u/ex-msg-and-stacktrace e))))))))))

(defn xf-ean->info [new-branch env-auth-name->info]
  (reduce-kv (fn [acc auth-name auth-info]
               (assoc acc auth-name
                      (-> auth-info
                          (assoc :authenticator-branch new-branch)
                          (assoc :authenticator-branch-source
                                 (:authenticator-branch auth-info)))))
             {}
             env-auth-name->info))

(defn xf-esr->info [new-branch env-sp-root->info]
  (reduce-kv (fn [acc root sp-info]
               (assoc acc root
                      (-> sp-info
                          (assoc :state-provider-branch new-branch)
                          (assoc :state-provider-branch-source
                                 (:state-provider-branch sp-info)))))
             {}
             env-sp-root->info))

(defn ->temp-env-info
  [{:keys [env-name->info env-params] :as arg}]
  (let [{:keys [env-name source-env-name env-lifetime-mins]} env-params
        source-env-info (env-name->info source-env-name)]
    (-> source-env-info
        (assoc :env-name env-name)
        (assoc :env-lifetime-mins env-lifetime-mins)
        (update :env-authenticator-name->info
                (partial xf-ean->info env-name))
        (update :env-sp-root->info
                (partial xf-esr->info env-name)))))

(defn make-client-on-connect
  [{:keys [*conn-id->auth-info
           *conn-id->env-name
           *conn-id->connected?
           *env-name->info]
    :as fn-arg}]
  (fn [{:keys [close! conn-id path remote-address] :as conn}]
    (ca/go
      (try
        (let [uri-map (uri/uri path)
              env-params (-> uri-map :query u/query-string->env-params)
              {:keys [env-name source-env-name]} env-params]
          (when-not env-name
            (throw (ex-info
                    (str "Must provide an :env-name in order to connect. "
                         "Got `" (or env-name "nil") "`.")
                    env-params)))
          (when (and source-env-name
                     (not (contains? @*env-name->info source-env-name)))
            (throw (ex-info
                    (str "source-env-name provided is not an existing env. "
                         "Got `" source-env-name "`. Valid envs are: "
                         (keys @*env-name->info))
                    env-params)))
          (when-not (contains? @*env-name->info env-name)
            ;; Create a temp env
            (swap! *env-name->info
                   (fn [env-name->info]
                     (let [env-info (->temp-env-info (u/sym-map env-name->info
                                                                env-params))]
                       (assoc env-name->info env-name env-info))))
            (au/<? (<copy-from-branch-sources!
                    {:env-info (@*env-name->info env-name)
                     :source-env-name source-env-name
                     :temp? true})))
          (swap! *conn-id->auth-info assoc conn-id {})
          (swap! *conn-id->env-name assoc conn-id env-name)
          (swap! *conn-id->connected? assoc conn-id true)
          (log/info
           (str "Client connection opened:\n"
                (u/pprint-str (u/sym-map conn-id env-name remote-address)))))
        (catch Exception e
          (log/error (str "Error in client on-connect:\n"
                          (u/ex-msg-and-stacktrace e)))
          (close!))))))

(defn make-client-on-disconnect
  [{:keys [*conn-id->auth-info
           *conn-id->env-name
           *conn-id->connected?
           *connected-actor-id->conn-ids]
    :as arg}]
  (fn [{:keys [conn-id]}]
    (swap! *conn-id->connected? assoc conn-id false)
    (when-let [actor-id (some-> @*conn-id->auth-info
                                (get conn-id)
                                (:actor-id))]
      (swap! *connected-actor-id->conn-ids update actor-id disj conn-id))
    (swap! *conn-id->auth-info dissoc conn-id)
    (swap! *conn-id->env-name dissoc conn-id)
    (log/info (str "Client connection closed:\n"
                   (u/pprint-str (u/sym-map conn-id))))))

(defn make-client-ep-info [{:keys [storage] :as arg}]
  {:handlers {:get-schema-pcf-for-fingerprint
              #(au/go
                 (-> (storage/<fp->schema storage (:arg %))
                     (au/<?)
                     (l/json)))

              :log-in
              (make-<auth-handler
               (assoc arg :<f auth-impl/<handle-log-in))

              :log-out
              (make-<auth-handler
               (assoc arg :<f auth-impl/<handle-log-out))

              :read-authenticator-state
              (make-<auth-handler
               (assoc arg :<f auth-impl/<handle-read-authenticator-state))


              :resume-login-session
              (make-<auth-handler
               (assoc arg :<f auth-impl/<handle-resume-login-session))

              :rpc
              (make-<rpc-handler arg)

              :state-provider-msg
              (make-<sp-msg-handler arg)

              :state-provider-rpc
              (make-<sp-rpc-handler arg)

              :update-authenticator-state
              (make-<auth-handler
               (assoc arg :<f auth-impl/<handle-update-authenticator-state))}
   :on-connect (make-client-on-connect arg)
   :on-disconnect (make-client-on-disconnect arg)
   :path "/client"
   :protocol schemas/client-server-protocol})

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

(defn make-talk2-server [{:keys [port] :as arg}]
  (let [server (t2s/server (assoc arg :port port))
        admin-client-ep-info (make-admin-client-ep-info
                              (assoc arg :talk2-server server))
        client-ep-info (make-client-ep-info arg)]
    (t2s/add-endpoint! (assoc admin-client-ep-info :server server))
    (t2s/add-endpoint! (assoc client-ep-info :server server))
    server))

(defn make-authenticator-storage [authenticator-name storage]
  (let [str-ns (namespace authenticator-name)
        str-name (name authenticator-name)
        prefix (if str-ns
                 (str str-ns "_" str-name)
                 str-name)]
    (storage/make-prefixed-storage prefix storage)))

(defn xf-authenticator-info [{:keys [authenticators storage]}]
  (reduce (fn [acc authenticator]
            (let [authenticator-name (auth-impl/get-name authenticator)
                  authenticator-storage (make-authenticator-storage
                                         authenticator-name
                                         storage)
                  info (u/sym-map authenticator
                                  authenticator-storage)]
              (assoc acc authenticator-name info)))
          {}
          authenticators))

(defn get-perm-env-name->info
  [{:keys [root->state-provider storage] :as arg}]
  (let [stored-m (au/<?? (storage/<get storage
                                       storage/env-name-to-info-key
                                       schemas/env-name-to-info-schema))
        default-spis (reduce-kv (fn [acc root sp]
                                  (let [sp-name (::sp-impl/state-provider-name
                                                 sp)]
                                    (conj acc {:path-root root
                                               :state-provider-name sp-name})))
                                []
                                root->state-provider)
        m (assoc stored-m u/default-env-name
                 {:env-name u/default-env-name
                  :stored-state-provider-infos default-spis})]
    (reduce-kv (fn [acc env-name info]
                 (assoc acc env-name
                        (xf-perm-env-info (assoc arg :env-info info))))
               {}
               m)))

;; TODO: DRY this up w/ com.oncurrent.zeno.client.impl/<rpc!*
(defn <rpc!*
  [{:keys [arg-schema conn-id ret-schema rpc-name-kw state-provider-name
           storage talk2-server rpc-msg-type]
    :as arg}]
  (au/go
    (let [rpc-id (u/compact-random-uuid)
          s-arg (au/<? (storage/<value->serialized-value
                        storage arg-schema (:arg arg)))
          rpc-arg {:arg s-arg
                   :rpc-id rpc-id
                   :rpc-name-kw-ns (namespace rpc-name-kw)
                   :rpc-name-kw-name (name rpc-name-kw)
                   :state-provider-name state-provider-name}
          timeout-ms (or (:timeout-ms arg) u/default-rpc-timeout-ms)
          rpc-ch (t2s/<send-msg! {:arg rpc-arg
                                  :conn-id conn-id
                                  :msg-type-name rpc-msg-type
                                  :server talk2-server
                                  :timeout-ms timeout-ms})
          timeout-ch (ca/timeout timeout-ms)
          [ret ch] (au/alts? [rpc-ch timeout-ch])
          <request-schema (su/make-schema-requester {:conn-id conn-id
                                                     :server talk2-server})]
      (cond
        (= timeout-ch ch)
        (throw (ex-info (str "RPC timed out. rpc-name-kw: `" rpc-name-kw "` "
                             "rpc-id: `" rpc-id "`.\n")
                        (u/sym-map rpc-id rpc-name-kw timeout-ms)))

        (= :zeno/rpc-error ret)
        (throw (ex-info (str "RPC failed. rpc-name-kw: `" rpc-name-kw "` "
                             "rpc-id: `" rpc-id "`.\n See server log for "
                             "more information.\n")
                        (u/sym-map rpc-id rpc-name-kw)))

        (= :zeno/rpc-unauthorized ret)
        (throw (ex-info (str "Unauthorized RPC. rpc-name-kw: `" rpc-name-kw "` "
                             "rpc-id: `" rpc-id "`.\n See server log for "
                             "more information.\n")
                        (u/sym-map rpc-id rpc-name-kw)))

        :else
        (au/<? (common/<serialized-value->value
                {:<request-schema <request-schema
                 :reader-schema ret-schema
                 :serialized-value ret
                 :storage storage}))))))

;; TODO: DRY this up w/ com.oncurrent.zeno.client.impl/<sp-send-msg
(defn <sp-send-msg
  [{:keys [arg conn-id msg-protocol msg-type state-provider-name
           storage talk2-server timeout-ms]}]
  (au/go
    (let [msg-info (msg-protocol msg-type)
          _ (when-not msg-info
              (throw (ex-info
                      (str "No msg type `" msg-type "` found in msg-protocol.")
                      {:msg-protocol-types (keys msg-protocol)
                       :msg-type msg-type})))
          {:keys [arg-schema ret-schema]} msg-info]
      (if ret-schema
        (au/<? (<rpc!* (-> (u/sym-map arg arg-schema conn-id ret-schema
                                      state-provider-name storage talk2-server
                                      timeout-ms)
                           (assoc :rpc-msg-type :state-provider-rpc)
                           (assoc :rpc-name-kw msg-type))))
        (let [s-arg (au/<? (storage/<value->serialized-value
                            storage arg-schema arg))
              msg-arg {:arg s-arg
                       :msg-type-ns (namespace msg-type)
                       :msg-type-name (name msg-type)
                       :state-provider-name state-provider-name}]
          (au/<? (t2s/<send-msg! {:arg msg-arg
                                  :conn-id conn-id
                                  :msg-type-name :state-provider-msg
                                  :server talk2-server
                                  :timeout-ms timeout-ms})))))))

(defn get-sp-branches [{:keys [*env-name->info root]}]
  (reduce-kv (fn [acc env-name info]
               (let [branch (some-> info
                                    :env-sp-root->info
                                    root
                                    :state-provider-branch)]
                 (if branch
                   (conj acc branch)
                   acc)))
             #{}
             @*env-name->info))

(defn <initialize-state-providers!
  [{:keys [*env-name->info bulk-storage root->state-provider
           storage talk2-server]}]
  (au/go
    (doseq [[root sp] root->state-provider]
      (let [{::sp-impl/keys [init! msg-protocol state-provider-name]} sp]
        (when init!
          (let [<send-msg (fn [{:keys [arg conn-id msg-type timeout-ms]}]
                            (<sp-send-msg (u/sym-map arg
                                                     conn-id
                                                     msg-protocol
                                                     msg-type
                                                     state-provider-name
                                                     storage
                                                     talk2-server
                                                     timeout-ms)))
                prefix (str storage/state-provider-prefix
                            (namespace state-provider-name) "-"
                            (name state-provider-name))
                branches (get-sp-branches (u/sym-map *env-name->info root))
                ret (init! {:<send-msg <send-msg
                            :branches branches
                            :bulk-storage (bulk-storage/->prefixed-bulk-storage
                                           (u/sym-map bulk-storage prefix))
                            :storage (storage/make-prefixed-storage
                                      prefix storage)})]
            (when (au/channel? ret)
              (au/<? ret))))))))

(defn make-name->state-provider [root->state-provider]
  (reduce-kv (fn [acc root state-provider]
               (assoc acc (::sp-impl/state-provider-name state-provider)
                      state-provider))
             {}
             root->state-provider))

;; TODO: Start a temp env GC loop

(defn ->zeno-server [config]
  (u/check-config {:config config
                   :config-type :server
                   :config-rules server-config-rules})
  (let [{:zeno/keys [<get-published-member-urls
                     <publish-member-urls
                     admin-password
                     authenticators
                     bulk-storage
                     certificate-str
                     port
                     private-key-str
                     root->state-provider
                     rpcs
                     storage
                     ws-url]} config
        *conn-id->auth-info (atom {})
        *conn-id->env-name (atom {})
        *conn-id->connected? (atom {})
        *connected-actor-id->conn-ids (atom {})
        *consumer-actor-id->last-branch-log-tx-index (atom {})
        *rpc-name-kw->handler (atom {})
        *logged-in-admin-conn-ids (atom #{})
        authenticator-name->info (xf-authenticator-info
                                  (u/sym-map authenticators storage))
        *env-name->info (atom (get-perm-env-name->info
                               (u/sym-map authenticator-name->info
                                          root->state-provider
                                          storage)))
        ;; TODO: What if we have two state providers w/ the same name
        ;; but different roots? Not handled properly currently.
        name->state-provider (make-name->state-provider root->state-provider)
        arg (u/sym-map *conn-id->auth-info
                       *conn-id->env-name
                       *conn-id->connected?
                       *connected-actor-id->conn-ids
                       *consumer-actor-id->last-branch-log-tx-index
                       *env-name->info
                       *logged-in-admin-conn-ids
                       *rpc-name-kw->handler
                       admin-password
                       authenticator-name->info
                       bulk-storage
                       certificate-str
                       name->state-provider
                       storage
                       port
                       private-key-str
                       root->state-provider
                       rpcs)
        talk2-server (make-talk2-server arg)]
    (au/<?? (<initialize-state-providers!
             (assoc arg :talk2-server talk2-server)))
    (u/sym-map *rpc-name-kw->handler
               bulk-storage
               storage
               root->state-provider
               rpcs
               talk2-server
               ws-url)))

(defn stop!
  [{:keys [bulk-storage root->state-provider talk2-server]}]
  (when talk2-server
    (t2s/stop! talk2-server))
  (au/<?? (bulk-storage/<stop-server! bulk-storage))
  (doseq [[root {::sp-impl/keys [stop!]}] root->state-provider]
    (when stop!
      (stop!))))

(defn set-rpc-handler! [zeno-server rpc-name-kw handler]
  (let [{:keys [*rpc-name-kw->handler]} zeno-server]
    (swap! *rpc-name-kw->handler assoc rpc-name-kw handler)))
