(ns oncurrent.zeno.server.authentication
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [oncurrent.zeno.common :as common]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.server.utils :as su]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)
   (java.security SecureRandom)))

(defprotocol IAuthenticator
  (<log-in! [this arg])
  (<log-out! [this arg])
  (<update-authenticator-state! [this arg])
  (get-login-info-schema [this])
  (get-login-ret-extra-info-schema [this])
  (get-name [this])
  (get-update-state-info-schema [this update-type])
  (get-update-state-ret-schema [this update-type]))

(defn generate-session-token []
  (let [rng (SecureRandom.)
        bytes (byte-array 32)]
    (.nextBytes rng bytes)
    (ba/byte-array->b64 bytes)))

(defn <handle-log-in
  [{:keys [*connection-info branch->authenticator-name->info storage] :as arg}]
  (au/go
    (let [{:keys [authenticator-name branch serialized-login-info]} (:arg arg)
          auth-info (some-> branch->authenticator-name->info
                            (get branch)
                            (get authenticator-name))
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found for branch id `" branch "`.")
                      (u/sym-map authenticator-name branch))))
          {:keys [authenticator authenticator-storage]} auth-info
          <request-schema (su/make-schema-requester arg)
          reader-schema (get-login-info-schema authenticator)
          login-info (au/<? (common/<serialized-value->value
                             {:<request-schema <request-schema
                              :reader-schema reader-schema
                              :serialized-value serialized-login-info
                              :storage storage}))
          login-ret (au/<? (<log-in! authenticator
                                     (u/sym-map authenticator-storage
                                                login-info)))]
      (when login-ret
        (let [{:keys [extra-info login-lifetime-mins actor-id]} login-ret
              _ (when (not (int? login-lifetime-mins))
                  (throw
                   (ex-info
                    (str "Authenticator `" authenticator-name "` did not "
                         "return an integer value for key "
                         "`:login-lifetime-mins` from `<log-in!`. Got `"
                         (or login-lifetime-mins "nil") "`.")
                    (u/sym-map login-lifetime-mins authenticator-name))))
              _ (when (not (string? actor-id))
                  (throw
                   (ex-info
                    (str "Authenticator `" authenticator-name "` did not "
                         "return a string value for key `:actor-id` from "
                         "`<log-in!`. Got `" (or actor-id "nil") "`.")
                    (u/sym-map login-lifetime-mins authenticator-name))))
              serialized-extra-info (when extra-info
                                      (au/<? (storage/<value->serialized-value
                                              storage
                                              (get-login-ret-extra-info-schema
                                               authenticator)
                                              extra-info)))
              session-token (generate-session-token)
              session-info {:session-token session-token
                            :session-token-minutes-remaining login-lifetime-mins
                            :actor-id actor-id}
              token-k (str storage/session-token-to-token-info-key-prefix
                           session-token)
              exp-ms (+ (u/current-time-ms) (* 60 1000 login-lifetime-mins))
              token-info {:session-token-expiration-time-ms exp-ms
                          :actor-id actor-id}]
          (au/<? (storage/<add! storage token-k
                                schemas/session-token-info-schema token-info))
          (swap! *connection-info
                 #(-> %
                      (assoc ::authenticator authenticator)
                      (assoc ::authenticator-storage authenticator-storage)
                      (assoc ::branch (:branch arg))
                      (assoc ::session-token session-token)
                      (assoc ::actor-id actor-id)))
          (u/sym-map serialized-extra-info session-info))))))

(defn <handle-log-out
  [{:keys [*connection-info storage]}]
  (au/go
    (let [conn-info @*connection-info
          token-k (str storage/session-token-to-token-info-key-prefix
                       (::session-token conn-info))]
      (au/<? (storage/<delete! storage token-k))
      (au/<? (<log-out! (::authenticator conn-info)
                        {:authenticator-storage
                         (::authenticator-storage conn-info)})))))

(defn <handle-resume-session
  [{:keys [*connection-info storage] :as arg}]
  (au/go
    (let [session-token (:arg arg)
          token-k (str storage/session-token-to-token-info-key-prefix
                       session-token)
          info (au/<? (storage/<get storage
                                    token-k
                                    schemas/session-token-info-schema))
          {:keys [session-token-expiration-time-ms actor-id]} info
          remaining-ms (when session-token-expiration-time-ms
                         (- session-token-expiration-time-ms
                            (u/current-time-ms)))
          session-token-minutes-remaining (when remaining-ms
                                            (u/floor-int
                                             (/ remaining-ms 1000 60)))]
      (if (and remaining-ms (pos? remaining-ms))
        (do
          (swap! *connection-info #(-> %
                                       (assoc ::session-token session-token)
                                       (assoc ::actor-id actor-id)))
          (u/sym-map session-token session-token-minutes-remaining actor-id))
        (do
          (au/<? (storage/<delete! storage token-k))
          nil)))))

(defn <handle-update-authenticator-state
  [{:keys [*connection-info branch->authenticator-name->info storage] :as arg}]
  ;; Client may or may not be logged in when this is called
  (au/go
    (let [{:keys [authenticator-name
                  branch
                  serialized-update-info
                  update-type]} (:arg arg)
          auth-info (some-> branch->authenticator-name->info
                            (get branch)
                            (get authenticator-name))
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found for branch id `" branch "`.")
                      (u/sym-map authenticator-name branch))))
          {:keys [authenticator authenticator-storage]} auth-info
          reader-schema (get-update-state-info-schema authenticator update-type)
          <request-schema (su/make-schema-requester arg)
          update-info (au/<? (common/<serialized-value->value
                              {:<request-schema <request-schema
                               :reader-schema reader-schema
                               :serialized-value serialized-update-info
                               :storage storage}))
          actor-id (::actor-id @*connection-info)
          ret (au/<? (<update-authenticator-state!
                      authenticator
                      (u/sym-map authenticator-storage
                                 actor-id
                                 update-info
                                 update-type)))]
      (au/<? (storage/<value->serialized-value
              storage
              (get-update-state-ret-schema authenticator update-type)
              ret)))))
