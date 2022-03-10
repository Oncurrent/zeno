(ns com.oncurrent.zeno.authenticators.identifier-secret.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.identifier-secret.shared :as shared]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authentication :as za]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(def identifier-to-actor-id-key-prefix "IDENTIFIER-TO-ACTOR-ID-")
(def actor-id-to-hashed-secret-key-prefix "_ACTOR-ID-TO-HASHED-SECRET-")
(def work-factor 12)

(defn <get-actor-id-for-identifier [authenticator-storage identifier]
  (let [k (str identifier-to-actor-id-key-prefix identifier)]
    (storage/<get authenticator-storage k schemas/actor-id-schema)))

(defn <get-hashed-secret-for-actor-id [authenticator-storage actor-id]
  (let [k (str actor-id-to-hashed-secret-key-prefix actor-id)]
    (storage/<get authenticator-storage k l/string-schema)))

(defn <log-in!* [{:keys [login-lifetime-mins authenticator-storage login-info]}]
  (au/go
    (let [identifier (-> login-info :identifier str/lower-case)
          actor-id (au/<? (<get-actor-id-for-identifier
                             authenticator-storage identifier))
          hashed-secret (when actor-id
                          (au/<? (<get-hashed-secret-for-actor-id
                                  authenticator-storage actor-id)))]
      (when (and hashed-secret
                 (bcrypt/check (:secret login-info) hashed-secret))
        ;; Can also return `:extra-info`, but we don't have any
        (u/sym-map login-lifetime-mins actor-id)))))

(defn <log-out!* [arg]
  (au/go
    ;; We don't need to do anything special on log out
    true))

(defmulti <update-authenticator-state!* :update-type)

(defmethod <update-authenticator-state!* :create-actor
  [{:keys [authenticator-storage update-info]}]
  (au/go
    (let [{:keys [identifier secret actor-id]
           :or {actor-id (u/compact-random-uuid)}} update-info
          ik (str identifier-to-actor-id-key-prefix identifier)
          sk (str actor-id-to-hashed-secret-key-prefix actor-id)]
      (try
        (au/<? (storage/<add! authenticator-storage ik schemas/actor-id-schema
                              actor-id))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Identifier `" identifier "` already exists.")
                    (u/sym-map identifier actor-id)))
            (throw e))))
      (try
        (au/<? (storage/<swap! authenticator-storage sk shared/secret-schema
                               (fn [old-hashed-secret]
                                 (when old-hashed-secret
                                   (throw (ex-info
                                           "Actor `" actor-id "` already has "
                                           "a secret set."
                                           {})))
                                 (bcrypt/encrypt secret work-factor))))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Actor `" actor-id "` already exists.")
                    (u/sym-map identifier actor-id)))
            (throw e))))
      actor-id)))

(defmethod <update-authenticator-state!* :add-identifier
  [{:keys [authenticator-storage actor-id] :as arg}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [identifier (:update-info arg)
          ik (str identifier-to-actor-id-key-prefix identifier)]
      (try
        (au/<? (storage/<add! authenticator-storage ik schemas/actor-id-schema
                              actor-id))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Identifier `" identifier "` already exists.")
                    (u/sym-map identifier actor-id)))
            (throw e))))
      true)))

(defmethod <update-authenticator-state!* :remove-identifier
  [{:keys [authenticator-storage actor-id] :as arg}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [identifier (:update-info arg)
          ik (str identifier-to-actor-id-key-prefix identifier)]
      (au/<? (storage/<delete! authenticator-storage ik))
      true)))

(defmethod <update-authenticator-state!* :set-secret
  [{:keys [authenticator-storage actor-id update-info]}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [{:keys [old-secret new-secret]} update-info
          sk (str actor-id-to-hashed-secret-key-prefix actor-id)]
      (au/<? (storage/<swap! authenticator-storage sk l/string-schema
                             (fn [old-hashed-secret]
                               (when-not
                                   (bcrypt/check old-secret old-hashed-secret)
                                 (throw (ex-info "Old secret is incorrect."
                                                 {})))
                               (bcrypt/encrypt new-secret work-factor))))
      true)))

(defrecord IdentifierSecretAuthenticator
  [login-lifetime-mins]
  za/IAuthenticator
  (<log-in! [this arg]
    (<log-in!* (assoc arg :login-lifetime-mins login-lifetime-mins)))
  (<log-out! [this arg]
    (<log-out!* arg))
  (<update-authenticator-state! [this arg]
    (<update-authenticator-state!* arg))
  (get-login-info-schema [this]
    shared/login-info-schema)
  (get-login-ret-extra-info-schema [this]
    l/null-schema)
  (get-name [this]
    shared/authenticator-name)
  (get-update-state-info-schema [this update-type]
    (case update-type
      :add-identifier shared/identifier-schema
      :create-actor shared/create-actor-info-schema
      :remove-identifier shared/identifier-schema
      :set-secret shared/set-secret-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-identifier l/boolean-schema
      :create-actor schemas/actor-id-schema
      :remove-identifier l/boolean-schema
      :set-secret l/boolean-schema)))

(defn make-authenticator
  ([]
   (make-authenticator {}))
  ([{:keys [login-lifetime-mins]
     :or {login-lifetime-mins (* 14 24 60)}}]
   (->IdentifierSecretAuthenticator login-lifetime-mins)))
