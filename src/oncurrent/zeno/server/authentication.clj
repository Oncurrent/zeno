(ns oncurrent.zeno.server.authentication
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
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
  (get-update-state-info-schema [this update-type])
  (get-update-state-ret-schema [this update-type]))

(defn generate-session-token []
  (let [rng (SecureRandom.)
        bytes (byte-array 32)]
    (.nextBytes rng bytes)
    (ba/byte-array->b64 bytes)))

(defn <handle-log-in
  [{:keys [*connection-info
           branch-id->authenticator-name->authenticator-info
           msg-arg
           storage]}]
  (au/go
    (let [{:keys [authenticator-name branch-id]} msg-arg
          auth-info (some-> branch-id->authenticator-name->authenticator-info
                            (get branch-id)
                            (get authenticator-name))
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found for branch id `" branch-id "`.")
                      (u/sym-map authenticator-name branch-id))))
          {:keys [authenticator authenticator-storage]} auth-info
          login-info (au/<? (storage/<serialized-value->value
                             storage
                             (get-login-info-schema authenticator)
                             (:serialized-login-info msg-arg)))
          login-ret (au/<? (<log-in! authenticator
                                     (u/sym-map authenticator-storage
                                                login-info)))]
      (when login-ret
        (let [{:keys [extra-info login-lifetime-mins subject-id]} login-ret
              _ (when (not (int? login-lifetime-mins))
                  (throw
                   (ex-info
                    (str "Authenticator `" authenticator-name "` did not "
                         "return an integer value for key "
                         "`:login-lifetime-mins` from `<log-in!`. Got `"
                         (or login-lifetime-mins "nil") "`.")
                    (u/sym-map login-lifetime-mins authenticator-name))))
              _ (when (not (string? subject-id))
                  (throw
                   (ex-info
                    (str "Authenticator `" authenticator-name "` did not "
                         "return a string value for key `:subject-id` from "
                         "`<log-in!`. Got `" (or subject-id "nil") "`.")
                    (u/sym-map login-lifetime-mins authenticator-name))))
              serialized-extra-info (when extra-info
                                      (au/<? (storage/<value->serialized-value
                                              storage
                                              (get-login-ret-extra-info-schema
                                               authenticator)
                                              extra-info)))
              sesion-token (generate-session-token)
              sesson-info {:session-token session-token
                           :session-token-minutes-remaining login-lifetime-mins
                           :subject-id subject-id}
              token-k (str storage/session-token-to-token-info-key-prefix
                           session-token)
              exp-ms (+ (u/current-time-ms) (* 60 1000 login-lifetime-mins))
              token-info {:session-token-expiration-time-ms exp-ms
                          :subject-id subject-id}]
          (au/<? (storage/<add! storage token-k
                                schemas/session-token-info-schema token-info))
          (swap! *connection-info
                 #(-> %
                      (assoc ::branch-id (:branch-id msg-arg))
                      (assoc ::session-token session-token)
                      (assoc ::subject-id subject-id)))
          (u/sym-map serialized-extra-info session-info))))))

(defn <handle-log-out
  [{:keys [*connection-info storage talk2server]}]
  (au/go
    (let [conn-info @*connection-info
          token-k (str storage/session-token-to-token-info-key-prefix
                       (::session-token conn-info))
          _ (au/<? (storage/<delete! storage token-k))
          ret (au/<? (<log-out! authenticator
                                (u/sym-map authenticator-storage)))]
      (t2s/close-connection! talk2server (:zeno/connection-id conn-info))
      ret)))

(defn <handle-resume-session
  [{:keys [*connection-info storage] :as arg}]
  (au/go
    (let [session-token (:msg-arg arg)
          token-k (str storage/session-token-to-token-info-key-prefix
                       session-token)
          info (au/<? (storage/<get storage
                                    token-k
                                    schemas/session-token-info-schema))
          {:keys [session-token-expiration-time-ms subject-id]} info
          remaining-ms (- session-token-expiration-time-ms (u/current-time-ms))
          session-token-minutes-remaining (u/floor-int
                                           (/ remaining-ms 1000 60))]
      (if (pos? remaining-ms)
        (do
          (swap! *connection-info #(-> %
                                       (assoc ::session-token session-token)
                                       (assoc ::subject-id subject-id)))
          (u/sym-map session-token session-token-minutes-remaining subject-id))
        (do
          (au/<? (storage/<delete! storage token-k))
          nil)))))

(defn <handle-update-authenticator-state
  [{:keys [*connection-info
           branch-id->authenticator-name->authenticator-info
           msg-arg
           storage] :as arg}]
  ;; We may or may not be logged in when this is called
  (au/go
    (let [{:keys [authenticator-name
                  branch-id
                  serialized-update-info
                  update-type]} msg-arg
          auth-info (some-> branch-id->authenticator-name->authenticator-info
                            (get branch-id)
                            (get authenticator-name))
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found for branch id `" branch-id "`.")
                      (u/sym-map authenticator-name branch-id))))
          {:keys [authenticator authenticator-storage]} auth-info
          update-info (au/<? (storage/<serialized-value->value
                              storage
                              (get-update-state-info-schema authenticator
                                                            update-type)
                              serialized-update-info))
          subject-id (::subject-id @*connection-info)
          ret (au/<? (<update-authenticator-state!
                      authenticator
                      (u/sym-map authenticator-storage
                                 subject-id
                                 update-info
                                 update-type)))]
      (au/<? (storage/<value->serialized-value
              storage
              (get-update-state-ret-schema authenticator update-type)
              ret)))))
