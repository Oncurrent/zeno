(ns com.oncurrent.zeno.admin-client
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.impl :as impl]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn admin-client
  "Returns a Zeno admin client.
   Required config keys:
    - `:admin-password` - The admin password of the server
    - `:get-server-url` - A fn that returns the server url as a string
    "
  [config]
  (impl/zeno-client config))

(defn stop!
  "Stop the admin client and its connection to the server.
   Mostly useful in tests."
  [zc]
  (impl/stop! zc))

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
                         "Got `" admin-client "`.")
                    (u/sym-map admin-client talk2-client))))
  (when-not (string? admin-password)
    (throw (ex-info (str "`:zeno/admin-client` must be a valid Admin Client. "
                         "The provided client is a valid Zeno Client, but "
                         "not a valid Admin Client.")
                    (u/sym-map admin-client)))))

;;;;;;;;;;;;;;;;;;;;;;;;; Public API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
don't use client impl for admin client

(defn <create-env!
  [{:zeno/keys [admin-client
                authenticator-infos
                env-name
                state-provider-infos]}]
  (check-authenticator-infos authenticator-infos)
  (check-env-name env-name)
  (check-state-provider-infos state-provider-infos)
  (check-admin-client admin-client)
  (t2c/<send-msg! (:talk2-client admin-client)
                  :create-env
                  (u/sym-map authenticator-infos
                             env-name
                             state-provider-infos)))

(defn <create-temporary-env!
  [{:zeno/keys [admin-client
                env-name
                lifetime-mins
                source-env-name]
    :as arg}]
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
  (t2c/<send-msg! admin-client :create-env (u/sym-map env-name
                                                      lifetime-mins
                                                      source-env-name)))

(defn <delete-env! [{:keys [admin-client
                            env-name]}]
  (check-env-name env-name)
  (t2c/<send-msg! admin-client :delete-env env-name))

(defn <get-env-names [{:keys [admin-client]}]
  (t2c/<send-msg! admin-client :get-env-names nil))
