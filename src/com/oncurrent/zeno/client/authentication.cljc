(ns com.oncurrent.zeno.client.authentication
  (:require
   [clojure.core.async :as ca]
   [com.deercreeklabs.talk2.client :as t2c]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-schema-requester [talk2-client]
  (fn [fp]
    (t2c/<send-msg! talk2-client :get-schema-pcf-for-fingerprint fp)))

(defn <client-log-in
  [{:keys [authenticator-name
           login-info
           login-info-schema
           zc]}]
  (au/go
    (let [{:keys [branch storage talk2-client]} zc
          ser-login-info (au/<? (storage/<value->serialized-value
                                 storage
                                 login-info-schema
                                 login-info))
          arg {:authenticator-name authenticator-name
               :branch branch
               :serialized-login-info ser-login-info}
          ret (au/<? (t2c/<send-msg! talk2-client :log-in arg))
          actor-id (some-> ret :session-info :actor-id)]
      (when actor-id
        (reset! (:*actor-id zc) actor-id))
      ret)))

(defn <client-log-out [{:keys [talk2-client *actor-id] :as zc}]
  ;; TODO: Delete stored transaction log data
  ;; TODO: Update subscribers that actor-id has changed
  (reset! *actor-id nil)
  (t2c/<send-msg! talk2-client :log-out nil))

(defn <client-update-authenticator-state
  [{:keys [authenticator-name
           return-value-schema
           update-info-schema
           update-info
           update-type
           zc]}]
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
    (let [{:keys [branch storage talk2-client]} zc
          ser-info (au/<? (storage/<value->serialized-value storage
                                                            update-info-schema
                                                            update-info))
          arg {:authenticator-name authenticator-name
               :branch branch
               :serialized-update-info ser-info
               :update-type update-type}
          s-val (au/<? (t2c/<send-msg! talk2-client
                                       :update-authenticator-state
                                       arg))
          <request-schema (make-schema-requester talk2-client)]
      (au/<? (common/<serialized-value->value
              {:<request-schema <request-schema
               :reader-schema return-value-schema
               :serialized-value s-val
               :storage storage})))))

(defn <client-resume-session
  [zc session-token]
  (t2c/<send-msg! (:talk2-client zc) :resume-session session-token))
