(ns com.oncurrent.zeno.authenticators.password.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.password.shared :as shared]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authenticator-impl :as za]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(def work-factor 12)

(defn <log-in!*
  [{:keys [<get-authenticator-state login-lifetime-mins login-info]}]
  (au/go
    (let [{:keys [actor-id password]} login-info
          authenticator-state (au/<? (<get-authenticator-state))
          hashed-password (get authenticator-state actor-id)]
      (when (and hashed-password
                 (bcrypt/check password hashed-password))
        ;; Can also return `:extra-info`, but we don't have any
        (u/sym-map login-lifetime-mins actor-id)))))

(defn <log-out!* [arg]
  (au/go
    ;; We don't need to do anything special on log out
    true))

(defmulti <update-authenticator-state!* :update-type)
(defmulti <read-authenticator-state* :get-type)

(defmethod <update-authenticator-state!* :add-actor-and-password
  [{:keys [<get-authenticator-state <swap-authenticator-state! update-info]}]
  (au/go
    (let [{:keys [actor-id password]} update-info
          actor-id (or (:actor-id update-info) (u/compact-random-uuid))]
      (au/<? (<swap-authenticator-state!
              (fn [old-state]
                (when (:actor-id old-state)
                  (throw (ex-info
                          (str "Actor `" actor-id "` already exists")
                          {})))
                (assoc old-state actor-id
                       (bcrypt/encrypt password work-factor)))))
      true)))

(defmethod <update-authenticator-state!* :set-password
  [{:keys [<swap-authenticator-state! actor-id update-info]}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [{:keys [old-password new-password]} update-info]
      (au/<? (<swap-authenticator-state!
              (fn [old-state]
                (when-not
                  (bcrypt/check old-password (get old-state actor-id))
                  (throw (ex-info "Old password is incorrect."
                                  {})))
                (assoc old-state actor-id
                       (bcrypt/encrypt new-password work-factor)))))
      true)))

(defrecord PasswordAuthenticator
    [login-lifetime-mins]
  za/IAuthenticator
  (<log-in! [this arg]
    (<log-in!* (assoc arg :login-lifetime-mins login-lifetime-mins)))
  (<log-out! [this arg]
    (<log-out!* arg))
  (<update-authenticator-state! [this arg]
    (<update-authenticator-state!* arg))
  (<read-authenticator-state [this arg]
    (<read-authenticator-state* arg))
  (get-login-info-schema [this]
    shared/login-info-schema)
  (get-login-ret-extra-info-schema [this]
    l/null-schema)
  (get-name [this]
    shared/authenticator-name)
  (get-schema [this]
    shared/storage-schema)
  (get-update-state-info-schema [this update-type]
    (case update-type
      :add-actor-and-password shared/add-actor-and-password-info-schema
      :set-password shared/set-password-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-actor-and-password l/boolean-schema
      :set-password l/boolean-schema))
  (get-read-state-info-schema [this get-type]
    )
  (get-read-state-ret-schema [this get-type]
    ))

(defn ->authenticator
  ([]
   (->authenticator {}))
  ([{:keys [login-lifetime-mins]
     :or {login-lifetime-mins (* 14 24 60)}}]
   (->PasswordAuthenticator login-lifetime-mins)))
