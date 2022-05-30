(ns com.oncurrent.zeno.client.impl
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.client :as aa]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;; TODO: Fill this in
(def default-config
  {})

;; TODO: Revise these
(def client-config-rules
  #:zeno{:get-server-base-url
         {:required? false
          :checks [{:pred ifn?
                    :msg "must be a function"}]}

         :env-name
         {:required? false
          :checks [{:pred string?
                    :msg "must be a string"}]}

         :root->state-provider
         {:required? false
          :checks [{:pred associative?
                    :msg "must be a associative"}]}

         :storage
         {:required? false
          :checks [{:pred #(satisfies? storage/IStorage %)
                    :msg "must satisfy the IStorage protocol"}]}})

(defn make-schema-requester [talk2-client]
  (fn [fp]
    (t2c/<send-msg! talk2-client :get-schema-pcf-for-fingerprint fp)))

(defn split-cmds [cmds]
  (reduce
   (fn [acc cmd]
     (common/check-cmd cmd)
     (let [{:zeno/keys [path]} cmd
           [head & tail] path]
       (update acc head #(conj (or % []) cmd))))
   {}
   cmds))

(defn <do-state-updates!*
  [{:keys [root->state-provider root->cmds]}]
  (au/go
    (let [roots (keys root->cmds)
          last-i (dec (count roots))]
      (if (empty? roots)
        []
        (loop [i 0
               out []]
          (let [root (nth roots i)
                state-provider (root->state-provider root)
                _ (when-not state-provider
                    (throw
                     (ex-info (str "No state provider found for root `"
                                   (or root "nil") "`.")
                              {:root root
                               :known-roots (keys root->state-provider)})))
                {::sp-impl/keys [<update-state!]} state-provider
                cmds (root->cmds root)
                {:keys [updated-paths]} (au/<? (<update-state! {:zeno/cmds cmds
                                                                :root root}))
                new-out (concat out updated-paths)]
            (if (= last-i i)
              new-out
              (recur (inc i)
                     new-out))))))))

(defn <do-update-state! [zc cmds]
  ;; This is called serially from the update-state loop.
  ;; We can rely on there being no concurrent updates.
  ;; We need to execute all the commands transactionally. Either they
  ;; all commit or none commit. A transaction may include many kinds of
  ;; updates.
  (au/go
    (let [{:keys [talk2-client]} zc
          updated-paths (au/<? (<do-state-updates!*
                                (assoc zc :root->cmds (split-cmds cmds))))]
      (state-subscriptions/do-subscription-updates! (u/sym-map zc update-paths))
      true)))

(defn start-update-state-loop! [zc]
  (ca/go
    (let [{:keys [*stop? update-state-ch]} zc]
      (loop []
        (try
          (let [[{:keys [cmds cb]} ch] (ca/alts! [update-state-ch
                                                  (ca/timeout 1000)])]
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
        (when-not @*stop?
          (recur))))))

(defn make-on-connect
  [{:keys [*connected? root->state-provider] :as arg}]
  (fn [{:keys [protocol url]}]
    (reset! *connected? true)
    (ca/go
      (try
        (doseq [[root {::sp-impl/keys [on-connect]}] root->state-provider]
          (when on-connect
            (on-connect url)))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in on-connect:\n"
                     (u/ex-msg-and-stacktrace e)))))))

(defn make-on-disconnect
  [{:keys [*connected? root->state-provider] :as arg}]
  (fn [{:keys [code url]}]
    (reset! *connected? false)
    (ca/go
      (try
        (doseq [[root {::sp-impl/keys [on-disconnect]}] root->state-provider]
          (when on-disconnect
            (on-disconnect code)))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error "Error in on-disconnect:\n"
                     (u/ex-msg-and-stacktrace e)))))))

(defn make-get-url [{:keys [get-server-base-url] :as arg}]
  (fn []
    (let [base-url (get-server-base-url)
          query-str (u/env-params->query-string arg)]
      (str base-url
           (when-not (str/ends-with? base-url "/")
             "/")
           "client"
           (when (not-empty query-str)
             "?")
           query-str))))

(defn make-talk2-client
  [{:keys [storage] :as arg}]
  (let [handlers {:get-schema-pcf-for-fingerprint
                  (fn [fn-arg]
                    (au/go
                      (-> (storage/<fp->schema storage (:arg fn-arg))
                          (au/<?)
                          (l/json))))}]
    (t2c/client {:get-url (make-get-url arg)
                 :handlers handlers
                 :on-connect (make-on-connect arg)
                 :on-disconnect (make-on-disconnect arg)
                 :protocol schemas/client-server-protocol})))

;; TODO: DRY this up w/ com.oncurrent.zeno.server/<rpc!*
(defn <rpc!*
  [{:keys [<request-schema arg-schema ret-schema rpc-name-kw state-provider-name
           storage talk2-client rpc-msg-type]
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
          rpc-ch (t2c/<send-msg! talk2-client rpc-msg-type rpc-arg)
          timeout-ms (or (:timeout-ms arg) u/default-rpc-timeout-ms)
          timeout-ch (ca/timeout timeout-ms)
          [ret ch] (au/alts? [rpc-ch timeout-ch])]
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

;; TODO: DRY this up w/ com.oncurrent.zeno.server/<sp-send-msg
(defn <sp-send-msg
  [{:keys [<request-schema arg msg-protocol msg-type state-provider-name
           storage talk2-client timeout-ms]}]
  (au/go
    (let [msg-info (msg-protocol msg-type)
          _ (when-not msg-info
              (throw (ex-info
                      (str "No msg type `" msg-type "` found in msg-protocol.")
                      {:msg-protocol-types (keys msg-protocol)
                       :msg-type msg-type})))
          {:keys [arg-schema ret-schema]} msg-info]
      (if ret-schema
        (au/<? (<rpc!* (-> (u/sym-map <request-schema arg arg-schema ret-schema
                                      state-provider-name storage
                                      talk2-client timeout-ms)
                           (assoc :rpc-msg-type :state-provider-rpc)
                           (assoc :rpc-name-kw msg-type))))
        (let [s-arg (au/<? (storage/<value->serialized-value
                            storage arg-schema arg))
              msg-arg {:arg s-arg
                       :msg-type-ns (namespace msg-type)
                       :msg-type-name (name msg-type)
                       :state-provider-name state-provider-name}]
          (au/<? (t2c/<send-msg! talk2-client :state-provider-msg msg-arg)))))))

(defn make-update-subscriptions! [zc]
  (fn [{:keys [updated-paths]}]
    (state-subscriptions/do-subscription-updates!
     (u/sym-map updated-paths zc))))

(defn initialize-state-providers!
  [{:keys [*connected? env-name root->state-provider storage talk2-client]
    :as fn-arg}]
  (doseq [[root sp] root->state-provider]
    (let [{::sp-impl/keys [init! msg-protocol state-provider-name]} sp]
      (when init!
        (let [<request-schema (make-schema-requester talk2-client)
              <send-msg (fn [{:keys [arg msg-type timeout-ms]}]
                          (<sp-send-msg (u/sym-map <request-schema
                                                   arg
                                                   msg-protocol
                                                   msg-type
                                                   state-provider-name
                                                   storage
                                                   talk2-client
                                                   timeout-ms)))
              prefix (str storage/state-provider-prefix
                          (namespace state-provider-name) "-"
                          (name state-provider-name))]
          (init! {:<request-schema <request-schema
                  :<send-msg <send-msg
                  :update-subscriptions! (make-update-subscriptions! fn-arg)
                  :connected? (fn []
                                @*connected?)
                  :env-name env-name
                  :storage (storage/make-prefixed-storage prefix storage)}))))))

(defn update-state! [zc cmds cb]
  ;; We put the updates on a channel to guarantee serial update order
  (ca/put! (:update-state-ch zc) (u/sym-map cmds cb)))

(defn zeno-client [config]
  (let [config* (-> (merge default-config config)
                    (u/fill-env-defaults))
        _ (u/check-config {:config config*
                           :config-type :client
                           :config-rules client-config-rules})
        {:zeno/keys [admin-password
                     client-name
                     env-lifetime-mins
                     env-name
                     get-server-base-url
                     root->state-provider
                     rpcs
                     source-env-name
                     storage]
         :or {storage (storage/make-storage
                       (storage/make-mem-raw-storage))}} config*
        *next-instance-num (atom 0)
        *next-topic-sub-id (atom 0)
        *topic-name->sub-id->cb (atom {})
        *stop? (atom false)
        *state-sub-name->info (atom {})
        *actor-id (atom nil)
        *talk2-client (atom nil)
        *connected? (atom false)
        ;; TODO: Is there a case where this would drop data?
        update-state-ch (ca/chan (ca/sliding-buffer 1000))
        arg (u/sym-map *actor-id
                       *connected?
                       *state-sub-name->info
                       *stop?
                       *talk2-client
                       admin-password
                       client-name
                       env-lifetime-mins
                       env-name
                       get-server-base-url
                       root->state-provider
                       rpcs
                       storage
                       source-env-name
                       update-state-ch)
        talk2-client (when (:zeno/get-server-base-url config*)
                       (make-talk2-client arg))
        _ (reset! *talk2-client talk2-client)
        _ (initialize-state-providers! (assoc arg :talk2-client talk2-client))
        <on-actor-id-change (fn [actor-id]
                              (au/go
                                (doseq [[root sp] root->state-provider]
                                  (when-let [<oaic (::sp-impl/<on-actor-id-change sp)]
                                    (au/<? (<oaic actor-id))))))
        zc (merge arg (u/sym-map *next-instance-num
                                 *next-topic-sub-id
                                 *topic-name->sub-id->cb
                                 <on-actor-id-change
                                 talk2-client))]
    (start-update-state-loop! zc)
    zc))

(defn stop! [{:keys [*stop? root->state-provider talk2-client] :as zc}]
  (reset! *stop? true)
  (when talk2-client
    (t2c/stop! talk2-client))
  (doseq [[root {::sp-impl/keys [stop!]}] root->state-provider]
    (when stop!
      (stop!))))

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
                         "be a string. Got `" (or topic-name "nil") "`.")
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

(defn logged-in?
  [zc]
  (boolean @(:*actor-id zc)))

(defn <rpc! [{:keys [rpc-name-kw zc] :as arg}]
  (when-not (:talk2-client zc)
    (throw
     (ex-info (str "Can't call `<rpc!` because "
                   "`:zeno/get-server-base-url` was not provided in "
                   "the client configuration.")
              {})))
  (when-not (keyword? rpc-name-kw)
    (throw (ex-info (str "The `rpc-name-kw` parameter must be a keyword. "
                         "Got `" (or rpc-name-kw "nil") "`.")
                    (u/sym-map rpc-name-kw arg))))
  (let [{:keys [rpcs storage talk2-client]} zc
        {:keys [arg-schema ret-schema]} (get rpcs rpc-name-kw)
        <request-schema (make-schema-requester talk2-client)]
    (<rpc!* (-> arg
                (assoc :<request-schema <request-schema)
                (assoc :arg-schema arg-schema)
                (assoc :ret-schema ret-schema)
                (assoc :rpc-msg-type :rpc)
                (assoc :storage storage)
                (assoc :talk2-client talk2-client)))))

(defn rpc! [{:keys [cb] :as arg}]
  (ca/go
    (try
      (let [ret (au/<? (<rpc! arg))]
        (when cb
          (cb ret)))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log/error (str "Exception in rpc!:\n" (u/ex-msg-and-stacktrace e)))
        (when cb
          (cb e))))))
