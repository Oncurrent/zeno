(ns oncurrent.zeno.authenticators.identifier-secret.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.authenticators.identifier-secret.shared :as shared]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.server.authentication :as za]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(set! *warn-on-reflection* true)

(def identifier-to-subject-id-key-prefix "IDENTIFIER-TO-SUBJECT-ID-")
(def subject-id-to-hashed-secret-key-prefix "_SUBJECT-ID-TO-HASHED-SECRET-")
(def work-factor 12)

(defn <get-subject-id-for-identifier [authenticator-storage identifier]
  (let [k (str identifier-to-subject-id-key-prefix identifier)]
    (storage/<get authenticator-storage k schemas/subject-id-schema)))

(defn <get-hashed-secret-for-subject-id [authenticator-storage subject-id]
  (let [k (str subject-id-to-hashed-secret-key-prefix subject-id)]
    (storage/<get authenticator-storage k l/string-schema)))

(defn <log-in!* [{:keys [login-lifetime-mins authenticator-storage login-info]}]
  (au/go
    (let [identifier (-> login-info :identifier str/lower-case)
          subject-id (au/<? (<get-subject-id-for-identifier
                             authenticator-storage identifier))
          hashed-secret (when subject-id
                          (au/<? (<get-hashed-secret-for-subject-id
                                  authenticator-storage subject-id)))]
      (when (and hashed-secret
                 (bcrypt/check (:secret login-info) hashed-secret))
        ;; Can also return `:extra-info`, but we don't have any
        (u/sym-map login-lifetime-mins subject-id)))))

(defn <log-out!* [arg]
  (au/go
    ;; We don't need to do anything special on log out
    true))

(defmulti <update-authenticator-state!* :update-type)

(defmethod <update-authenticator-state!* :create-subject
  [{:keys [authenticator-storage update-info]}]
  (au/go
    (let [{:keys [identifier secret subject-id]} update-info
          ik (str identifier-to-subject-id-key-prefix identifier)
          sk (str subject-id-to-hashed-secret-key-prefix subject-id)
          hashed-secret (bcrypt/encrypt secret work-factor)]
      (try
        (au/<? (storage/<add! authenticator-storage ik schemas/subject-id-schema
                              subject-id))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Identifier `" identifier "` already exists.")
                    (u/sym-map identifier subject-id)))
            (throw e))))
      (try
        (au/<? (storage/<add! authenticator-storage sk l/string-schema
                              hashed-secret))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Subject `" subject-id "` already exists.")
                    (u/sym-map identifier subject-id)))
            (throw e))))
      true)))

(defmethod <update-authenticator-state!* :add-identifier
  [{:keys [authenticator-storage subject-id] :as arg}]
  (au/go
    (when-not subject-id
      (throw (ex-info "Subject is not logged in." {})))
    (let [identifier (:update-info arg)
          ik (str identifier-to-subject-id-key-prefix identifier)]
      (try
        (au/<? (storage/<add! authenticator-storage ik schemas/subject-id-schema
                              subject-id))
        (catch ExceptionInfo e
          (if (= :key-exists (some-> e ex-data :type))
            (throw (ex-info
                    (str "Identifier `" identifier "` already exists.")
                    (u/sym-map identifier subject-id)))
            (throw e))))
      true)))

(defmethod <update-authenticator-state!* :remove-identifier
  [{:keys [authenticator-storage subject-id] :as arg}]
  (au/go
    (when-not subject-id
      (throw (ex-info "Subject is not logged in." {})))
    (let [identifier (:update-info arg)
          ik (str identifier-to-subject-id-key-prefix identifier)]
      (au/<? (storage/<delete! authenticator-storage ik))
      true)))

(defmethod <update-authenticator-state!* :set-secret
  [{:keys [authenticator-storage subject-id update-info]}]
  (au/go
    (when-not subject-id
      (throw (ex-info "Subject is not logged in." {})))
    (let [{:keys [identifier old-secret new-secret]} update-info
          sk (str subject-id-to-hashed-secret-key-prefix subject-id)]
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
  (get-update-state-info-schema [this update-type]
    (case update-type
      :add-identifier shared/identifier-schema
      :create-subject shared/create-subject-info-schema
      :remove-identifier shared/identifier-schema
      :set-secret shared/set-secret-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-identifier l/boolean-schema
      :create-subject schemas/subject-id-schema
      :remove-identifier l/boolean-schema
      :set-secret l/boolean-schema)))

(defn make-authenticator
  [{:keys [login-lifetime-mins]
    :or {login-lifetime-mins (* 14 24 60)}}]
  (->IdentifierSecretAuthenticator login-lifetime-mins))
