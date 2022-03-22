(ns com.oncurrent.zeno.authenticators.magic-token.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.magic-token.shared :as shared]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.server.authentication :as za]
   [com.oncurrent.zeno.server.utils :as su]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (at.favre.lib.crypto.bcrypt BCrypt BCrypt$Hasher)
   (clojure.lang ExceptionInfo)
   (java.util Base64 Base64$Encoder)))

(set! *warn-on-reflection* true)

(def identifier-to-actor-id-key-prefix "IDENTIFIER-TO-ACTOR-ID-")
(def hashed-token-to-info-key-prefix "_HASHED-TOKEN-TO-INFO-")
(def work-factor 12)
(def salt (ba/byte-array 16 (range 16)))
(def url-safe-b64-encoder (.withoutPadding (Base64/getUrlEncoder)))
(def default-mins-valid* 15)
(def default-number-of-uses* 1)

(defn nil-or-pred? [x p]
  (or (nil? x) (p x)))

(defn number-of-mins->epoch-ms [mins]
  (when mins
    (+ (u/current-time-ms) (* mins 60 1000))))

(defn generate-token
  "Generate a secure random url-safe token."
  []
  (.encodeToString
   ^Base64$Encoder url-safe-b64-encoder
   (su/secure-random-bytes 32)))

(defn encrypt-const-salt
  "Uses the same underlying BCrypt as weavejester/crypto-password (which we use
   directly elsewhere e.g. for secrets in the identifier-secret authentication
   plugin). Since this plugin uses randomly generated tokens they are not in
   danger of so called rainbow tables. Also we store information keyed by the
   hash of the token so we need to be able to hash the token the same each time
   rather than having random salt generated each time."
  [raw]
  (String. (.hash (BCrypt/withDefaults)
                  work-factor salt (.getBytes ^String raw))))

(defn identifier-key [identifier]
  (str identifier-to-actor-id-key-prefix identifier))

(defn hashed-token-key [hashed-token]
  (str hashed-token-to-info-key-prefix hashed-token))

(defn <dec-remaining-uses! [authenticator-storage hashed-token]
  (storage/<swap! authenticator-storage
                  (hashed-token-key hashed-token)
                  shared/magic-token-info-schema
                  (fn [old-token-info]
                    (update old-token-info :remaining-uses dec))))

;; Can provide default values for keys for mins-valid and number-of-uses as
;; corresponding keys in the record that implements this protocol.
(defprotocol IMagicTokenApplicationServer
  (get-extra-info-schema [this])
  (get-params-schema [this])
  (<handle-request-magic-token! [this arg])
  (<handle-redeem-magic-token! [this arg]))

(defn <with-deserialized-extra-info
  [{:keys [serialized-extra-info] :as token-info}
   {:keys [authenticator-storage mtas] :as arg}]
  (au/go
   (-> token-info
       (assoc :extra-info
              (when serialized-extra-info
                (au/<? (common/<serialized-value->value
                        {:<request-schema (su/make-schema-requester arg)
                         :reader-schema (get-extra-info-schema mtas)
                         :serialized-value serialized-extra-info
                         :storage authenticator-storage}))))
       (dissoc :serialized-extra-info))))

(defn <log-in!* [{token :login-info
                  :keys [login-lifetime-mins authenticator-storage mtas]
                  :as arg}]
  (au/go
   (let [hashed-token (encrypt-const-salt token)
         token-info (au/<? (storage/<get authenticator-storage
                                         (hashed-token-key hashed-token)
                                         shared/magic-token-info-schema))]
     (when-let [{:keys [actor-id identifier expiration-ms remaining-uses
                        serialized-extra-info]} token-info]
       (let [actor-id* (au/<? (storage/<get authenticator-storage
                                            (identifier-key identifier)
                                            schemas/actor-id-schema))
             same-actor? (= actor-id actor-id*)
             active? (nil-or-pred? expiration-ms #(> % (u/current-time-ms)))
             more-uses? (nil-or-pred? remaining-uses #(> % 0))]
         (when (and active? more-uses? same-actor?)
           (when remaining-uses
             (au/<? (<dec-remaining-uses! authenticator-storage
                                          hashed-token)))
           (au/<? (<handle-redeem-magic-token!
                   mtas
                   (merge arg
                          {:token token
                           :token-info (au/<? (<with-deserialized-extra-info
                                               token-info arg))})))
           (assoc (u/sym-map login-lifetime-mins actor-id)
                  :extra-info token-info)))))))

(defn <log-out!* [arg]
  (au/go
   ;; Don't need to do anything on logout. Decrementing :remaining-uses and
   ;; the deletion of expired or used up tokens happens on log-in.
   true))

(defmulti <update-authenticator-state!* :update-type)

(defn request-magic-token-info->magic-token-info
  [{{:keys [identifier mins-valid number-of-uses] :as info} :update-info
    {:keys [default-mins-valid default-number-of-uses]} :mtas
    :keys [actor-id requestor-id]}]
  (-> info
      (assoc :actor-id actor-id)
      (assoc :requestor-id requestor-id)
      (assoc :expiration-ms (number-of-mins->epoch-ms
                             (or mins-valid
                                 default-mins-valid
                                 default-mins-valid*)))
      (assoc :remaining-uses (or number-of-uses
                                 default-number-of-uses
                                 default-number-of-uses*))
      (dissoc :serialized-params)))

(defn <add-identifier* [{:keys [authenticator-storage actor-id identifier]}]
  (au/go
   (try
    (let [actor-id (or actor-id (u/compact-random-uuid))]
      (au/<? (storage/<add! authenticator-storage
                            (identifier-key identifier)
                            schemas/actor-id-schema
                            actor-id))
      actor-id)
    (catch ExceptionInfo e
      (if (= :key-exists (some-> e ex-data :type))
        (throw (ex-info
                (str "identifier `" identifier "` already exists.")
                (u/sym-map actor-id identifier)))
        (throw e))))))

(defmethod <update-authenticator-state!* :request-magic-token
  [{:keys [actor-id authenticator-storage update-info mtas] :as arg}]
  (au/go
   (let [{:keys [identifier serialized-params]} update-info
         token (generate-token)
         hashed-token (encrypt-const-salt token)
         stored-actor-id (or (au/<? (storage/<get authenticator-storage
                                                  (identifier-key identifier)
                                                  schemas/actor-id-schema))
                             (au/<? (<add-identifier*
                                     (u/sym-map authenticator-storage
                                                identifier))))
         token-info (request-magic-token-info->magic-token-info
                     (assoc (u/sym-map update-info mtas)
                            :actor-id stored-actor-id
                            :requestor-id actor-id))]
     (au/<? (storage/<swap! authenticator-storage
                            (hashed-token-key hashed-token)
                            shared/magic-token-info-schema
                            (fn [old-token-info]
                              (when old-token-info
                                (throw (ex-info "Token already in use." {})))
                              token-info)))
     (au/<? (<handle-request-magic-token!
             mtas (merge arg
                         {:token token
                          :token-info (au/<? (<with-deserialized-extra-info
                                              token-info arg))
                          :params (au/<?
                                   (common/<serialized-value->value
                                    {:<request-schema (su/make-schema-requester
                                                       arg)
                                     :reader-schema (get-params-schema mtas)
                                     :serialized-value serialized-params
                                     :storage authenticator-storage}))})))
     true)))

(defmethod <update-authenticator-state!* :create-actor
  [{:keys [authenticator-storage update-info]}]
  (let [{:keys [actor-id identifier]} update-info]
    (<add-identifier* (u/sym-map authenticator-storage actor-id
                                 identifier))))

(defmethod <update-authenticator-state!* :add-identifier
  [{:keys [authenticator-storage actor-id update-info]}]
  (au/go
   (when-not actor-id
     (throw (ex-info "Actor is not logged in." {})))
   (au/<? (<add-identifier* (assoc (u/sym-map authenticator-storage actor-id)
                                   :identifier update-info)))
   true))

(defmethod <update-authenticator-state!* :remove-identifier
  [{:keys [authenticator-storage actor-id update-info] :as arg}]
  (au/go
   (when-not actor-id
     (throw (ex-info "Actor is not logged in." {})))
   (au/<? (storage/<delete! authenticator-storage
                            (identifier-key update-info)))
   true))

(defrecord MagicTokenAuthenticator
  [login-lifetime-mins storage-name mtas]
  za/IAuthenticator
  (<log-in! [this arg]
    (<log-in!* (merge this arg)))
  (<log-out! [this arg]
    (<log-out!* (merge this arg)))
  (<update-authenticator-state! [this arg]
    (<update-authenticator-state!* (merge this arg)))
  (get-login-info-schema [this]
    shared/magic-token-schema)
  (get-login-ret-extra-info-schema [this]
    shared/magic-token-info-schema)
  (get-name [this]
    shared/authenticator-name)
  (get-update-state-info-schema [this update-type]
    (case update-type
      :add-identifier shared/identifier-schema
      :create-actor shared/create-actor-info-schema
      :remove-identifier shared/identifier-schema
      :request-magic-token shared/request-magic-token-info-schema))
  (get-update-state-ret-schema [this update-type]
    (case update-type
      :add-identifier l/boolean-schema
      :create-actor schemas/actor-id-schema
      :remove-identifier l/boolean-schema
      :request-magic-token l/boolean-schema)))

(defn make-authenticator
  [{:keys [login-lifetime-mins storage-name mtas]
    :or {login-lifetime-mins (* 14 24 60)}}]
  (when-not (or (nil? storage-name) (keyword? storage-name))
    (throw (ex-info (str "The supplied storage-name must be a keyword or nil. Got "
                         storage-name " which is a " (type storage-name) ". "))))
  (when-not (satisfies? IMagicTokenApplicationServer mtas)
    (throw (ex-info (str "The supplied MagicTokenApplicationServer (mtas) does "
                         "not satisfy the IMagicTokenApplicationServer protocol.")
                    (u/sym-map mtas))))
  (->MagicTokenAuthenticator
   login-lifetime-mins storage-name mtas))
