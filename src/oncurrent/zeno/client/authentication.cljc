(ns oncurrent.zeno.client.authentication
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <client-log-in
  [{:keys [authenticator-name
           login-info
           login-info-schema
           zc]}]
  (au/go
    (let [{:keys [branch-id capsule-client storage]} zc
          ser-login-info (au/<? (storage/<value->serialized-value
                                 storage
                                 login-info-schema
                                 login-info))
          arg {:authenticator-name authenticator-name
               :branch-id branch-id
               :serialized-login-info ser-login-info}
          ret (au/<? (cc/<send-msg capsule-client :log-in arg))
          subject-id (some-> ret :session-info :subject-id)
          _ (when subject-id
              (reset! (:*subject-id zc) subject-id))]
      ret)))

(defn <client-log-out [zc]
  ;; TODO: Delete stored transaction log data
  (reset! (:*subject-id zc) nil)
  (cc/<send-msg capsule-client :log-out nil))

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
    (let [{:keys [branch-id capsule-client storage]} zc
          ser-info (au/<? (storage/<value->serialized-value storage
                                                            update-info-schema
                                                            update-info))
          arg {:authenticator-name authenticator-name
               :branch-id branch-id
               :serialized-update-info ser-info
               :update-type update-type}
          ret (au/<? (cc/<send-msg capsule-client
                                   :update-authenticator-state arg))]
      (au/<? (storage/<serialized-value->value storage
                                               return-value-schema
                                               ret)))))

(defn <client-resume-session
  [zc session-token]
  (let [{:keys [capsule-client]} zc]
    (cc/<send-msg capsule-client :resume-session session-token)))
