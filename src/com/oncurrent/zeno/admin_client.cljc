(ns com.oncurrent.zeno.admin-client
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.impl :as impl]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn check-authenticator-infos [authenticator-infos]
  (doseq [{:zeno/keys [authenticator-name
                       authenticator-branch]
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
                      (u/sym-map authenticator-branch authenticator-infos)))))
  (reduce (fn [acc authenticator-info]
            (if-not (acc authenticator-info)
              (conj acc authenticator-info)
              (throw (ex-info
                      (str "`:zeno/authenticator-infos` must be unique. `"
                           authenticator-info "` was duplicated.")
                      (u/sym-map authenticator-info authenticator-infos)))))
          #{}
          authenticator-infos))

(defn check-env-name [env-name]
  (when-not (string? env-name)
    (throw (ex-info (str "`:zeno/env-name` must be a string. "
                         "Got `" (or env-name "nil") "`.")
                    (u/sym-map env-name)))))

(defn check-state-provider-infos [state-provider-infos]
  (doseq [{:zeno/keys [path-root
                       state-provider-name
                       state-provider-branch]
           :as state-provider-info}
          state-provider-infos]
    (when-not (keyword? path-root)
      (throw (ex-info (str "`:zeno/path-root` must be a keyword. "
                           "Got `" (or path-root "nil") "`.")
                      (u/sym-map path-root state-provider-infos))))
    (when-not (keyword? state-provider-name)
      (throw (ex-info (str "`:zeno/state-provider-name` must be a keyword. "
                           "Got `" (or state-provider-name "nil") "`.")
                      (u/sym-map state-provider-name state-provider-infos))))
    (when (and state-provider-branch
               (not (string? state-provider-branch)))
      (throw (ex-info (str "`:zeno/state-provider-branch` must be a string. "
                           "Got `" state-provider-branch "`.")
                      (u/sym-map state-provider-branch state-provider-infos)))))
  (reduce (fn [acc {:zeno/keys [path-root]}]
            (if-not (acc path-root)
              (conj acc path-root)
              (throw (ex-info
                      (str "`:zeno/path-root`s in `:zeno/state-provider-infos "
                           "must be unique. `" path-root "` was duplicated.")
                      (u/sym-map path-root state-provider-infos)))))
          #{}
          state-provider-infos))

(defn check-admin-client
  [{:keys [admin-password talk2-client] :as admin-client}]
  (when-not talk2-client
    (throw (ex-info (str "`:zeno/admin-client` must be a valid Admin Client. "
                         "Got `" (or admin-client "nil") "`.")
                    (u/sym-map admin-client talk2-client)))))

(defn <check-login [{:keys [*login-status max-wait-ms]}]
  (au/go
    (let [expiry-ms (+ max-wait-ms (u/current-time-ms))]
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

(defn make-admin-client [{:zeno/keys [admin-password get-server-url]}]
  (when-not (string? admin-password)
    (throw (ex-info (str "`:zeno/admin-password` must be a string. Got `"
                         (or admin-password "nil") "`.")
                    (u/sym-map admin-password))))
  (let [*talk2-client (atom nil)
        *login-status (atom :waiting)
        on-connect (make-on-connect
                    (u/sym-map *login-status *talk2-client admin-password))
        on-disconnect (fn [{:keys [code url]}]
                        (when (= :logged-in @*login-status)
                          (reset! *login-status :waiting)))
        talk2-client (t2c/client
                      {:get-url get-server-url
                       :on-connect on-connect
                       :on-disconnect on-disconnect
                       :protocol schemas/admin-client-server-protocol})]
    (reset! *talk2-client talk2-client)
    (u/sym-map talk2-client)))

;;;;;;;;;;;;;;;;;;;;;;;;; Public API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn admin-client
  "Returns a Zeno admin client.
   Required config keys:
    - `:zeno/admin-password` - The admin password of the server
    - `:zeno/get-server-url` - A fn that returns the server url as a string
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
    (check-authenticator-infos authenticator-infos)
    (check-env-name env-name)
    (check-state-provider-infos state-provider-infos)
    (check-admin-client admin-client)
    (au/<? (<check-login))
    (au/<? (t2c/<send-msg! (:talk2-client admin-client)
                           :create-env
                           (u/sym-map authenticator-infos
                                      env-name
                                      state-provider-infos)))))

(defn <create-temporary-env!
  [{:zeno/keys [admin-client
                env-name
                lifetime-mins
                source-env-name]
    :as arg}]
  (au/go
    (check-env-name env-name)
    (when-not (integer? lifetime-mins)
      (throw (ex-info (str "`:zeno/lifetime-mins` must be an integer. "
                           "Got `" (or lifetime-mins "nil") "`.")
                      (u/sym-map lifetime-mins arg))))
    (when (and source-env-name
               (not (string? source-env-name)))
      (throw (ex-info (str "`:zeno/source-env-name` must be a string. "
                           "Got `" source-env-name "`.")
                      (u/sym-map arg source-env-name))))
    (au/<? (<check-login))
    (au/<? (t2c/<send-msg! admin-client
                           :create-env
                           (u/sym-map env-name
                                      lifetime-mins
                                      source-env-name)))))

(defn <delete-env! [{:keys [admin-client
                            env-name]}]
  (au/go
    (check-env-name env-name)
    (au/<? (<check-login))
    (au/<? (t2c/<send-msg! admin-client :delete-env env-name))))

(defn <get-env-names [{:keys [admin-client]}]
  (au/go
    (au/<? (<check-login))
    (au/<? (t2c/<send-msg! admin-client :get-env-names nil))))
