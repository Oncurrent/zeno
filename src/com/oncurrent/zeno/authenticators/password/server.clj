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
(defmulti <get-authenticator-state* :get-type)

(defmethod <update-authenticator-state!* :create-actor
  [{:keys [authenticator-storage update-info]}]
  (au/go
    (let [identifier (-> update-info :identifier str/lower-case)
          secret (:secret update-info)
          actor-id (or (:actor-id update-info) (u/compact-random-uuid))
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
    (let [identifier (-> arg :update-info str/lower-case)
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
    (let [identifier (-> arg :update-info str/lower-case)
          ik (str identifier-to-actor-id-key-prefix identifier)]
      (au/<? (storage/<delete! authenticator-storage ik))
      true)))

(defmethod <update-authenticator-state!* :set-secret
  [{:keys [authenticator-storage actor-id check-old? update-info]
    :or {check-old? true}}]
  (au/go
    (when-not actor-id
      (throw (ex-info "Actor is not logged in." {})))
    (let [{:keys [old-secret new-secret]} update-info
          sk (str actor-id-to-hashed-secret-key-prefix actor-id)]
      (au/<? (storage/<swap! authenticator-storage sk l/string-schema
                             (fn [old-hashed-secret]
                               (when check-old?
                                 (when-not
                                   (bcrypt/check old-secret old-hashed-secret)
                                   (throw (ex-info "Old secret is incorrect."
                                                   {}))))
                               (bcrypt/encrypt new-secret work-factor))))
      true)))

(defmethod <get-authenticator-state* :is-identifier-taken
  [{identifier :get-info :keys [authenticator-storage]}]
  (au/go
   (->> identifier
        (str/lower-case)
        (<get-actor-id-for-identifier authenticator-storage)
        (au/<?)
        (boolean))))

(defrecord IdentifierSecretAuthenticator
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
      :add-identifier shared/identifier-schema
      :create-actor shared/create-actor-info-schema
      :remove-identifier shared/identifier-schema
      :set-secret shared/set-secret-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-identifier l/boolean-schema
      :create-actor schemas/actor-id-schema
      :remove-identifier l/boolean-schema
      :set-secret l/boolean-schema))
  (get-get-state-info-schema [this get-type]
    (case get-type
      :is-identifier-taken shared/identifier-schema))
  (get-get-state-ret-schema [this get-type]
    (case get-type
      :is-identifier-taken l/boolean-schema)))

(defn make-authenticator
  ([]
   (make-authenticator {}))
  ([{:keys [login-lifetime-mins storage-name]
     :or {login-lifetime-mins (* 14 24 60)}}]
   (when-not (or (nil? storage-name) (keyword? storage-name))
     (throw (ex-info (str "The supplied storage-name must be a keyword or nil. Got "
                          storage-name " which is a " (type storage-name) ". "))))
   (->IdentifierSecretAuthenticator login-lifetime-mins storage-name)))