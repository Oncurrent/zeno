(ns com.oncurrent.zeno.authenticators.password.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client.authenticator-impl :as ai]
   [com.oncurrent.zeno.authenticators.password :as pwd-auth]
   [com.oncurrent.zeno.authenticators.password.shared :as shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn check-actor-id [actor-id]
  (when-not (string? actor-id)
    (throw (ex-info (str "`:zeno/actor-id` must be a string. Got `"
                         (or actor-id "nil") "`.")
                    (u/sym-map actor-id)))))

(defn check-zeno-client [zeno-client]
  (when-not (associative? zeno-client)
    (throw (ex-info (str "`:zeno/zeno-client` must be a valid Zeno client. "
                         "Got `" (or zeno-client "nil") "`.")
                    (u/sym-map zeno-client)))))

(defn <add-actor-and-password!
  "Returns a boolean success value."
  [{:zeno/keys [actor-id zeno-client]
    ::pwd-auth/keys [password]
    :as arg}]
  (check-actor-id actor-id)
  (check-zeno-client zeno-client)
  (when-not (string? password)
    (throw (ex-info (str "`:password` must be a string. Got `"
                         (or password "nil") "`.")
                    (u/sym-map password actor-id))))
  (ai/<client-update-authenticator-state
   {:authenticator-name shared/authenticator-name
    :return-value-schema l/boolean-schema
    :update-info-schema shared/add-actor-and-password-info-schema
    :update-info (u/sym-map actor-id password)
    :update-type :add-actor-and-password
    :zeno-client zeno-client}))

(defn <set-password!
  "Returns a boolean success value."
  [{:zeno/keys [actor-id zeno-client]
    ::pwd-auth/keys [new-password old-password]}]
  (check-actor-id actor-id)
  (check-zeno-client zeno-client)
  (when-not (string? old-password)
    (throw (ex-info (str "`:old-password` must be a string. Got `"
                         (or old-password "nil") "`.")
                    (u/sym-map old-password actor-id))))
  (when-not (string? new-password)
    (throw (ex-info (str "`:new-password` must be a string. Got `"
                         (or new-password "nil") "`.")
                    (u/sym-map new-password actor-id))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :update-info-schema shared/set-password-info-schema
             :update-info (u/sym-map actor-id new-password old-password)
             :update-type :set-password
             :zeno-client zeno-client}]
    (ai/<client-update-authenticator-state arg)))

(defn <log-in!
  "Returns a boolean success value."
  [{:zeno/keys [actor-id zeno-client]
    ::pwd-auth/keys [password]}]
  (au/go
    (check-actor-id actor-id)
    (check-zeno-client zeno-client)
    (when-not (string? password)
      (throw (ex-info (str "`:password` must be a string. Got `"
                           (or password "nil") "`.")
                      (u/sym-map password actor-id))))
    (let [arg {:authenticator-name shared/authenticator-name
               :login-info (u/sym-map actor-id password)
               :login-info-schema shared/login-info-schema
               :zeno-client zeno-client}
          ret (au/<? (ai/<client-log-in arg))]
      (or (:login-session-info ret)
          false))))

(defn <log-out!
  "Returns a boolean success value."
  [{:zeno/keys [zeno-client]}]
  (ai/<client-log-out zeno-client))

(defn <resume-login-session!
  [{:zeno/keys [login-session-token zeno-client]}]
  (au/go
    (let [ret (au/<? (ai/<client-resume-login-session
                      (u/sym-map login-session-token zeno-client)))]
      (or ret false))))
