(ns com.oncurrent.zeno.client.authenticator-impl
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client.impl :as cimpl]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn set-actor-id! [{:keys [*actor-id actor-id on-actor-id-change]}]
  (reset! *actor-id actor-id)
  (on-actor-id-change actor-id))

(defn <client-log-in
  [{:keys [authenticator-name
           login-info
           login-info-schema
           login-ret-extra-info-schema
           zeno-client]}]
  (au/go
    (let [{:keys [crdt-branch storage talk2-client]} zeno-client
          ser-login-info (au/<? (storage/<value->serialized-value
                                 storage
                                 login-info-schema
                                 login-info))
          arg {:authenticator-name authenticator-name
               :branch crdt-branch
               :serialized-login-info ser-login-info}
          ret (au/<? (t2c/<send-msg! talk2-client :log-in arg))
          {:keys [serialized-extra-info login-session-info]} ret
          <request-schema (cimpl/make-schema-requester talk2-client)
          extra-info (when (and login-ret-extra-info-schema
                                serialized-extra-info)
                       (au/<? (common/<serialized-value->value
                               {:<request-schema <request-schema
                                :reader-schema login-ret-extra-info-schema
                                :serialized-value serialized-extra-info
                                :storage storage})))]
      (u/sym-map extra-info login-session-info))))

(defn <client-log-out [{:keys [*actor-id storage talk2-client] :as zeno-client}]
  ;; TODO: Delete stored transaction log data
  ;; TODO: Update subscribers that actor-id has changed
  (set-actor-id! (assoc zeno-client :actor-id nil))
  (t2c/<send-msg! talk2-client :log-out nil))

(defn <client-update-authenticator-state
  [{:keys [authenticator-name
           return-value-schema
           update-info-schema
           update-info
           update-type
           zeno-client]}]
  (when-not (keyword? authenticator-name)
    (throw (ex-info (str "`authenticator-name` must be a keyword. Got `"
                         (or authenticator-name "nil") "`.")
                    (u/sym-map authenticator-name update-type update-info))))
  (when-not (l/schema? return-value-schema)
    (throw (ex-info
            (str "`return-value-schema` must be a Lancaster schema. Got `"
                 (or return-value-schema "nil") "`.")
            (u/sym-map authenticator-name return-value-schema update-type
                       update-info))))
  (when-not (l/schema? update-info-schema)
    (throw (ex-info
            (str "`update-info-schema` must be a Lancaster schema. Got `"
                 (or update-info-schema "nil") "`.")
            (u/sym-map authenticator-name update-info-schema update-type
                       update-info))))
  (au/go
    (let [{:keys [crdt-branch storage talk2-client]} zeno-client
          ser-info (au/<? (storage/<value->serialized-value storage
                                                            update-info-schema
                                                            update-info))
          arg {:authenticator-name authenticator-name
               :branch crdt-branch
               :serialized-update-info ser-info
               :update-type update-type}
          s-val (au/<? (t2c/<send-msg! talk2-client
                                       :update-authenticator-state
                                       arg))]
      (au/<? (common/<serialized-value->value
              {:<request-schema (cimpl/make-schema-requester talk2-client)
               :reader-schema return-value-schema
               :serialized-value s-val
               :storage storage})))))

(defn <client-get-authenticator-state
  [{:keys [authenticator-name
           return-value-schema
           get-info-schema
           get-info
           get-type
           zeno-client]}]
  (when-not (keyword? authenticator-name)
    (throw (ex-info (str "`authenticator-name` must be a keyword. Got `"
                         (or authenticator-name "nil") "`.")
                    (u/sym-map authenticator-name get-type get-info))))
  (when-not (l/schema? return-value-schema)
    (throw (ex-info
            (str "`return-value-schema` must be a Lancaster schema. Got `"
                 (or return-value-schema "nil") "`.")
            (u/sym-map authenticator-name return-value-schema get-type
                       get-info))))
  (when-not (l/schema? get-info-schema)
    (throw (ex-info
            (str "`get-info-schema` must be a Lancaster schema. Got `"
                 (or get-info-schema "nil") "`.")
            (u/sym-map authenticator-name get-info-schema get-type
                       get-info))))
  (au/go
    (let [{:keys [crdt-branch storage talk2-client]} zeno-client
          ser-info (au/<? (storage/<value->serialized-value storage
                                                            get-info-schema
                                                            get-info))
          arg {:authenticator-name authenticator-name
               :branch crdt-branch
               :serialized-get-info ser-info
               :get-type get-type}
          s-val (au/<? (t2c/<send-msg! talk2-client
                                       :get-authenticator-state
                                       arg))]
      (au/<? (common/<serialized-value->value
              {:<request-schema (cimpl/make-schema-requester talk2-client)
               :reader-schema return-value-schema
               :serialized-value s-val
               :storage storage})))))

(defn <client-resume-login-session
  [{:keys [login-session-token zeno-client]}]
  (t2c/<send-msg! (:talk2-client zeno-client)
                  :resume-login-session
                  login-session-token))
