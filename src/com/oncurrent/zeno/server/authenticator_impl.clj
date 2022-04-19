(ns com.oncurrent.zeno.server.authenticator-impl
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(defprotocol IAuthenticator
  (<log-in! [this arg])
  (<log-out! [this arg])
  (<update-authenticator-state! [this arg])
  (<read-authenticator-state [this arg])
  (get-login-info-schema [this])
  (get-login-ret-extra-info-schema [this])
  (get-name [this])
  (get-schema [this])
  (get-update-state-info-schema [this update-type])
  (get-update-state-ret-schema [this update-type])
  (get-read-state-info-schema [this read-type])
  (get-read-state-ret-schema [this read-type]))

(defn generate-login-session-token []
  (ba/byte-array->b64 (su/secure-random-bytes 32)))

(defn branch->key [branch]
  (str "_" branch))

(defn make-<get-authenticator-state
  [{:keys [authenticator authenticator-storage authenticator-branch branch]}]
  (fn []
    (storage/<get authenticator-storage
                  (branch->key (or branch authenticator-branch))
                  (get-schema authenticator))))

(defn make-<swap-authenticator-state!
  [{:keys [authenticator authenticator-storage authenticator-branch branch]}]
  (fn [update-fn]
    (storage/<swap! authenticator-storage
                    (branch->key (or branch authenticator-branch))
                    (get-schema authenticator)
                    update-fn)))

(defn <handle-log-in
  [{:keys [*conn-id->auth-info *connected-actor-id->conn-ids
           <get-state <set-state! <update-state!
           arg env-authenticator-name->info conn-id env-name storage]
    :as fn-arg}]
  (au/go
    (let [{:keys [authenticator-name serialized-login-info]} arg
          auth-info (env-authenticator-name->info authenticator-name)
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found in this env.")
                      (u/sym-map authenticator-name env-name))))
          <get-authenticator-state (make-<get-authenticator-state auth-info)
          {:keys [authenticator
                  authenticator-branch
                  authenticator-storage]} auth-info
          <request-schema (su/make-schema-requester fn-arg)
          reader-schema (get-login-info-schema authenticator)
          login-info (au/<? (common/<serialized-value->value
                             {:<request-schema <request-schema
                              :reader-schema reader-schema
                              :serialized-value serialized-login-info
                              :storage storage}))
          login-ret (au/<? (<log-in! authenticator
                                     (u/sym-map <get-authenticator-state
                                                <get-state
                                                <set-state!
                                                <update-state!
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
              login-session-token (generate-login-session-token)
              login-session-info {:login-session-token login-session-token

                                  :login-session-token-minutes-remaining
                                  login-lifetime-mins

                                  :actor-id actor-id}
              token-k (str storage/login-session-token-to-token-info-key-prefix
                           login-session-token)
              exp-ms (+ (u/current-time-ms) (* 60 1000 login-lifetime-mins))
              token-info {:login-session-token-expiration-time-ms exp-ms
                          :actor-id actor-id}]
          (au/<? (storage/<add! storage token-k
                                schemas/login-session-token-info-schema
                                token-info))
          (swap! *conn-id->auth-info
                 (fn [m]
                   (update m conn-id
                           #(-> %
                                (assoc :authenticator authenticator)
                                (assoc :authenticator-storage
                                       authenticator-storage)
                                (assoc :authenticator-branch
                                       authenticator-branch)
                                (assoc :login-session-token
                                       login-session-token)
                                (assoc :actor-id actor-id)))))
          (swap! *connected-actor-id->conn-ids
                 #(update % actor-id (fn [conn-ids]
                                       (conj (or conn-ids #{})
                                             conn-id))))
          (u/sym-map serialized-extra-info login-session-info))))))

(defn <handle-log-out
  [{:keys [*conn-id->auth-info <get-state <set-state! <update-state!
           conn-id storage]}]
  (au/go
    (let [auth-info (some-> @*conn-id->auth-info
                            (get conn-id))
          <get-authenticator-state (make-<get-authenticator-state auth-info)
          token-k (str storage/login-session-token-to-token-info-key-prefix
                       (:login-session-token auth-info))]
      (au/<? (storage/<delete! storage token-k))
      (au/<? (<log-out! (:authenticator auth-info)
                        (u/sym-map <get-authenticator-state
                                   <get-state
                                   <set-state!
                                   <update-state!))))))

(defn <handle-resume-login-session
  [{:keys [*conn-id->auth-info *connected-actor-id->conn-ids
           arg conn-id storage]
    :as fn-arg}]
  (au/go
    (let [login-session-token arg
          token-k (str storage/login-session-token-to-token-info-key-prefix
                       login-session-token)
          info (au/<? (storage/<get storage
                                    token-k
                                    schemas/login-session-token-info-schema))
          {:keys [login-session-token-expiration-time-ms actor-id]} info
          remaining-ms (when login-session-token-expiration-time-ms
                         (- login-session-token-expiration-time-ms
                            (u/current-time-ms)))
          login-session-token-minutes-remaining (when remaining-ms
                                                  (u/floor-int
                                                   (/ remaining-ms 1000 60)))]
      (if (and remaining-ms (pos? remaining-ms))
        (do
          (swap! *conn-id->auth-info
                 (fn [m]
                   (update m conn-id
                           #(-> %
                                (assoc :login-session-token
                                       login-session-token)
                                (assoc :actor-id actor-id)))))
          (swap! *connected-actor-id->conn-ids
                 #(update % actor-id (fn [conn-ids]
                                       (conj (or conn-ids #{})
                                             conn-id))))
          (u/sym-map actor-id
                     login-session-token
                     login-session-token-minutes-remaining))
        (do
          (au/<? (storage/<delete! storage token-k))
          nil)))))

(defn <handle-update-authenticator-state
  [{:keys [*conn-id->auth-info arg <get-state <set-state! <update-state!
           env-authenticator-name->info env-name conn-id storage]
    :as fn-arg}]
  ;; Client may or may not be logged in when this is called
  (au/go
    (let [{:keys [authenticator-name
                  serialized-update-info
                  update-type]} arg
          auth-info (env-authenticator-name->info authenticator-name)
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found in this env.")
                      (u/sym-map authenticator-name env-name))))
          <get-authenticator-state (make-<get-authenticator-state auth-info)
          <swap-authenticator-state! (make-<swap-authenticator-state! auth-info)
          {:keys [authenticator
                  authenticator-branch
                  authenticator-storage]} auth-info
          reader-schema (get-update-state-info-schema authenticator update-type)
          <request-schema (su/make-schema-requester fn-arg)
          update-info (au/<? (common/<serialized-value->value
                              {:<request-schema <request-schema
                               :reader-schema reader-schema
                               :serialized-value serialized-update-info
                               :storage storage}))
          {:keys [actor-id]} (some-> @*conn-id->auth-info
                                     (get conn-id))
          ret (au/<? (<update-authenticator-state!
                      authenticator
                      (u/sym-map <get-authenticator-state
                                 <swap-authenticator-state!
                                 <get-state
                                 <set-state!
                                 <update-state!
                                 actor-id
                                 update-info
                                 update-type)))]
      (au/<? (storage/<value->serialized-value
              storage
              (get-update-state-ret-schema authenticator update-type)
              ret)))))

(defn <handle-read-authenticator-state
  [{:keys [*conn-id->auth-info <get-state <set-state! <update-state!
           arg env-authenticator-name->info conn-id storage]
    :as fn-arg}]
  ;; Client may or may not be logged in when this is called)
  (au/go
    (let [{:keys [authenticator-name
                  serialized-read-info
                  read-type]} arg
          auth-info (env-authenticator-name->info authenticator-name)
          _ (when-not auth-info
              (throw (ex-info
                      (str "No authenticator with name `" authenticator-name
                           "` was found.")
                      (u/sym-map authenticator-name))))
          <get-authenticator-state (make-<get-authenticator-state auth-info)
          {:keys [authenticator
                  authenticator-branch
                  authenticator-storage]} auth-info
          reader-schema (get-read-state-info-schema authenticator read-type)
          <request-schema (su/make-schema-requester fn-arg)
          read-info (au/<? (common/<serialized-value->value
                           {:<request-schema <request-schema
                            :reader-schema reader-schema
                            :serialized-value serialized-read-info
                            :storage storage}))
          {:keys [actor-id]} (some-> @*conn-id->auth-info
                                     (get conn-id))
          ret (au/<? (<read-authenticator-state
                      authenticator
                      (u/sym-map <get-authenticator-state
                                 <get-state
                                 <set-state!
                                 <update-state!
                                 actor-id
                                 read-info
                                 read-type)))]
      (au/<? (storage/<value->serialized-value
              storage
              (get-read-state-ret-schema authenticator read-type)
              ret)))))

;; I should have the authenticator-storage available and the different branches
;; are just different keys so copying should be easy.
(defn <copy-branch!
  [{:keys [authenticator authenticator-storage
           authenticator-branch authenticator-branch-source]}]
  (au/go
   (when authenticator-branch-source
     (let [arg (u/sym-map authenticator authenticator-storage)
           <get* (make-<get-authenticator-state
                  (assoc arg :authenticator-branch
                         authenticator-branch-source))
           <swap!* (make-<swap-authenticator-state!
                    (assoc arg :authenticator-branch authenticator-branch))
           src-state (au/<? (<get*))]
       (log/info "COPYING")
       (au/<?
        (<swap!*
         (fn [old-state]
           (if-not (empty? old-state)
             (throw
              (ex-info
               (str "Authenticator branch `" authenticator-branch "` must "
                    "be empty in order to be populated from the source "
                    "branch `" authenticator-branch-source"`.")
               (u/sym-map authenticator-branch authenticator-branch-source)))
             src-state))))))))
