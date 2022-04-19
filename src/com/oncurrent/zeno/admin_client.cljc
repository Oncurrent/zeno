(ns com.oncurrent.zeno.admin-client
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.impl :as impl]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def default-max-login-wait-ms 20000)

(defn xf-authenticator-infos [authenticator-infos]
  (doseq [{:zeno/keys [authenticator-name
                       authenticator-branch
                       authenticator-branch-source]
           :as authenticator-info}
          authenticator-infos]
    (when-not (keyword? authenticator-name)
      (throw (ex-info (str "`:zeno/authenticator-name` must be a keyword. "
                           "Got `" (or authenticator-name "nil") "`.")
                      (u/sym-map authenticator-name authenticator-infos))))
    (when (and authenticator-branch
               (not (string? authenticator-branch)))
      (throw (ex-info (str "`:zeno/authenticator-branch` must be a string. "
                           "Got `" authenticator-branch "`.")
                      (u/sym-map authenticator-branch authenticator-infos))))
    (when (and authenticator-branch-source
               (not (string? authenticator-branch-source)))
      (throw (ex-info (str "`:zeno/authenticator-branch-source` must be a "
                           "string. Got `" authenticator-branch-source "`.")
                      (u/sym-map authenticator-branch-source
                                 authenticator-infos)))))
  (reduce
   (fn [acc {:zeno/keys [authenticator-name authenticator-branch
                         authenticator-branch-source]
             :as authenticator-info}]
     (let [u-select #(select-keys % [:authenticator-name])
           unique? (fn [x coll] (some #(= (u-select x) (u-select %)) coll))
           no-ns-info (u/sym-map authenticator-name authenticator-branch
                                 authenticator-branch-source)]
       (if-not (unique? no-ns-info acc)
         (conj acc no-ns-info)
         (throw
          (ex-info
           (str "`name/branch in :zeno/authenticator-infos` must be "
                "unique. `" authenticator-info "` was duplicated.")
           (u/sym-map authenticator-info authenticator-infos))))))
   []
   authenticator-infos))

(defn check-env-name [env-name]
  (when-not (string? env-name)
    (throw (ex-info (str "`:zeno/env-name` must be a string. "
                         "Got `" (or env-name "nil") "`.")
                    (u/sym-map env-name))))
  ;; TODO: Check that env-name is URL safe
  )

(defn xf-state-provider-infos [state-provider-infos]
  (reduce
   (fn [acc {:zeno/keys [path-root
                         state-provider-branch
                         state-provider-name]
             :as state-provider-info}]
     (when-not (keyword? path-root)
       (throw (ex-info (str "`:zeno/path-root` must be a keyword. "
                            "Got `" (or path-root "nil") "`.")
                       (u/sym-map path-root state-provider-infos))))
     (when (and state-provider-branch
                (not (string? state-provider-branch)))
       (throw (ex-info
               (str "`:zeno/state-provider-branch` must be a string. "
                    "Got `" state-provider-branch "`.")
               (u/sym-map state-provider-branch state-provider-infos))))
     (when-not (keyword? state-provider-name)
       (throw (ex-info
               (str "`:zeno/state-provider-name` must be a keyword. "
                    "Got `" (or state-provider-name "nil") "`.")
               (u/sym-map state-provider-name state-provider-infos))))
     (let [no-ns-info (u/sym-map path-root
                                 state-provider-branch
                                 state-provider-name)]
       (if-not  (some #(= path-root (:path-root %)) acc)
         (conj acc no-ns-info)
         (throw (ex-info
                 (str "`:zeno/path-root`s in `:zeno/state-provider-infos "
                      "must be unique. `" path-root "` was duplicated.")
                 (u/sym-map path-root state-provider-infos))))))
   []
   state-provider-infos))

(defn check-admin-client
  [{:keys [admin-password talk2-client] :as admin-client}]
  (when-not talk2-client
    (throw (ex-info (str "`:zeno/admin-client` must be a valid Admin Client. "
                         "Got `" (or admin-client "nil") "`.")
                    (u/sym-map admin-client talk2-client)))))

(defn <check-login [{:keys [*login-status max-login-wait-ms]}]
  (au/go
    (let [expiry-ms (+ (or max-login-wait-ms default-max-login-wait-ms)
                       (u/current-time-ms))]
      (loop []
        (case @*login-status
          :logged-in
          true

          :waiting
          (do
            (ca/<! (ca/timeout 10))
            (when (< (u/current-time-ms) expiry-ms)
              (recur)))

          :failed
          (throw (ex-info "Admin login failed" {})))))))

(defn make-on-connect [{:keys [*talk2-client *login-status admin-password]}]
  (fn [{:keys [protocol url]}]
    (ca/go
      (try
        (if (au/<? (t2c/<send-msg! @*talk2-client :log-in admin-password))
          (reset! *login-status :logged-in)
          (do
            (reset! *login-status :failed)
            (throw (ex-info "Admin login failed" {}))))
        (catch #?(:clj Exception :cljs js/Error) e
          (t2c/stop! @*talk2-client)
          (log/error "Error in on-connect:\n"
                     (u/ex-msg-and-stacktrace e)))))))

(defn make-admin-client
  [{:zeno/keys [admin-password get-server-base-url max-login-wait-ms]}]
  (when-not (string? admin-password)
    (throw (ex-info (str "`:zeno/admin-password` must be a string. Got `"
                         (or admin-password "nil") "`.")
                    (u/sym-map admin-password))))
  (when-not (ifn? get-server-base-url)
    (throw (ex-info (str "`:zeno/get-server-base-url` must be a function. Got `"
                         (or get-server-base-url "nil") "`.")
                    (u/sym-map get-server-base-url))))
  (let [*talk2-client (atom nil)
        *login-status (atom :waiting)
        on-connect (make-on-connect
                    (u/sym-map *login-status *talk2-client admin-password))
        on-disconnect (fn [{:keys [code url]}]
                        (when (= :logged-in @*login-status)
                          (reset! *login-status :waiting)))
        talk2-client (t2c/client
                      {:get-url get-server-base-url
                       :on-connect on-connect
                       :on-disconnect on-disconnect
                       :protocol schemas/admin-client-server-protocol})]
    (reset! *talk2-client talk2-client)
    (u/sym-map *login-status max-login-wait-ms talk2-client)))

;;;;;;;;;;;;;;;;;;;;;;;;; Public API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->admin-client
  "Returns a Zeno admin client.
   Required config keys:
    - `:zeno/admin-password` - The admin password of the server
    - `:zeno/get-server-base-url` - A fn that returns the server base url
    "
  [config]
  (make-admin-client config))

(defn stop!
  "Stop the admin client and its connection to the server.
   Mostly useful in tests."
  [{:keys [talk2-client] :as admin-client}]
  (t2c/stop! talk2-client))

(defn <create-env!
  [{:zeno/keys [admin-client
                authenticator-infos
                env-name
                state-provider-infos]}]
  (au/go
    (check-env-name env-name)
    (check-admin-client admin-client)
    (au/<? (<check-login admin-client))
    (let [ais (xf-authenticator-infos authenticator-infos)
          spis (xf-state-provider-infos state-provider-infos)]
      (au/<? (t2c/<send-msg! (:talk2-client admin-client)
                             :create-env
                             {:stored-authenticator-infos ais
                              :env-name env-name
                              :stored-state-provider-infos spis})))))

(defn <get-env-names
  [{:zeno/keys [admin-client]}]
  (au/go
    (check-admin-client admin-client)
    (au/<? (<check-login admin-client))
    (au/<? (t2c/<send-msg! (:talk2-client admin-client) :get-env-names nil))))

(defn <delete-env! [{:zeno/keys [admin-client
                                 env-name]}]
  (au/go
    (check-env-name env-name)
    (au/<? (<check-login admin-client))
    (au/<? (t2c/<send-msg! (:talk2-client admin-client) :delete-env env-name))))
