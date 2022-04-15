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

(def actor-id-to-hashed-password-key-prefix "_ACTOR-ID-TO-HASHED-PASSWORD-")
(def work-factor 12)

(defn <get-hashed-password-for-actor-id [authenticator-storage actor-id]
  (let [k (str actor-id-to-hashed-password-key-prefix actor-id)]
    (storage/<get authenticator-storage k l/string-schema)))

(defn <log-in!*
  [{:keys [authenticator-branch
           authenticator-storage
           login-lifetime-mins
           login-info]}]
  (au/go
    (let [{:keys [actor-id password]} login-info
          hashed-password (when actor-id
                            (au/<? (<get-hashed-password-for-actor-id
                                    authenticator-storage actor-id)))]
      (when (and hashed-password
                 (bcrypt/check password hashed-password))
        ;; Can also return `:extra-info`, but we don't have any
        (u/sym-map login-lifetime-mins actor-id)))))

(defn <log-out!* [arg]
  (au/go
    ;; We don't need to do anything special on log out
    true))

(defmulti <update-authenticator-state!* :update-type)
(defmulti <get-authenticator-state* :get-type)

(defmethod <update-authenticator-state!* :add-actor-and-password
  [{:keys [authenticator-storage update-info]}]
  (au/go
    (let [{:keys [actor-id password]} update-info
          actor-id (or (:actor-id update-info) (u/compact-random-uuid))
          sk (str actor-id-to-hashed-password-key-prefix actor-id)
          pwd-fn (fn [old-hashed-password]
                   (when old-hashed-password
                     (throw (ex-info
                             (str"Actor `" actor-id "` already exists")
                             {})))
                   (bcrypt/encrypt password work-factor))]
      (au/<? (storage/<swap! authenticator-storage sk shared/password-schema
                             pwd-fn))
      true)))

(defmethod <update-authenticator-state!* :set-password
  [{:keys [authenticator-storage actor-id update-info]}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [{:keys [old-password new-password]} update-info
          sk (str actor-id-to-hashed-password-key-prefix actor-id)]
      (au/<? (storage/<swap! authenticator-storage sk l/string-schema
                             (fn [old-hashed-password]
                               (when-not
                                   (bcrypt/check old-password
                                                 old-hashed-password)
                                 (throw (ex-info "Old password is incorrect."
                                                 {})))
                               (bcrypt/encrypt new-password work-factor))))
      true)))

(defrecord PasswordAuthenticator
    [login-lifetime-mins storage-name]
  za/IAuthenticator
  (<log-in! [this arg]
    (<log-in!* (assoc arg :login-lifetime-mins login-lifetime-mins)))
  (<log-out! [this arg]
    (<log-out!* arg))
  (<update-authenticator-state! [this arg]
    (<update-authenticator-state!* arg))
  (<get-authenticator-state [this arg]
    (<get-authenticator-state* arg))
  (get-login-info-schema [this]
    shared/login-info-schema)
  (get-login-ret-extra-info-schema [this]
    l/null-schema)
  (get-name [this]
    shared/authenticator-name)
  (get-update-state-info-schema [this update-type]
    (case update-type
      :add-actor-and-password shared/add-actor-and-password-info-schema
      :set-password shared/set-password-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-actor-and-password l/boolean-schema
      :set-password l/boolean-schema))
  (get-get-state-info-schema [this get-type]
    )
  (get-get-state-ret-schema [this get-type]
    ))

(defn ->authenticator
  ([]
   (->authenticator {}))
  ([{:keys [login-lifetime-mins storage-name]
     :or {login-lifetime-mins (* 14 24 60)}}]
   (when-not (or (nil? storage-name) (keyword? storage-name))
     (throw (ex-info
             (str "The supplied storage-name must be a keyword or nil. Got "
                  storage-name " which is a " (type storage-name) ". "))))
   (->PasswordAuthenticator login-lifetime-mins storage-name)))
