(ns com.oncurrent.zeno.authenticators.magic-token.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.magic-token.shared :as shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.client.authentication :as za]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <send-magic-token!
  "Returns a boolean success value."
  [zc {:keys [identifier mins-valid number-of-uses extra-info extra-info-schema]
       :as send-magic-token-info}]
  (au/go
    (when-not (string? identifier)
      (throw (ex-info (str "`identifier` must be a string. Got `"
                           (or identifier "nil") "`.")
                      (u/sym-map identifier))))
    (when (and extra-info (not extra-info-schema))
      (throw (ex-info "extra-info was provided with no extra-info-schema."
                      (u/sym-map extra-info))))
    (when (and extra-info-schema (not (l/schema? extra-info-schema)))
      (throw (ex-info "extra-info-schema was provided but is not a Lancaster schema."
                      (u/sym-map extra-info-schema))))
    (let [send-magic-token-info*
          (if-not (and extra-info extra-info-schema)
            send-magic-token-info
            (assoc send-magic-token-info :serialized-extra-info
                   (au/<? (storage/<value->serialized-value
                            (:storage zc) extra-info-schema extra-info))))
          arg {:authenticator-name shared/authenticator-name
               :return-value-schema l/boolean-schema
               :update-info-schema shared/send-magic-token-info-schema
               :update-info send-magic-token-info*
               :update-type :send-magic-token
               :zc zc}]
      (au/<? (za/<client-update-authenticator-state arg))
      true)))

(defn <redeem-magic-token!
  ([zc token]
   (<redeem-magic-token! zc token nil))
  ([zc token extra-info-schema]
   (au/go
     (when-not (string? token)
       (throw (ex-info (str "`token` must be a string. Got `"
                            (or token "nil") "`.")
                       (u/sym-map token))))
     (when (and extra-info-schema (not (l/schema? extra-info-schema)))
       (throw (ex-info "extra-info-schema was provided but is not a Lancaster schema."
                       (u/sym-map extra-info-schema))))
     (let [arg {:authenticator-name shared/authenticator-name
                :login-info token
                :login-info-schema shared/token-schema
                :zc zc}
           ret (au/<? (za/<client-log-in arg))
           ;; Note that the plubming can return extra-info which has a naming
           ;; collision with this plugin's extra-info. In this case we use the
           ;; plumbing's extra-info to return the token-info which includes
           ;; our plugin's serialized-extra-info, so we perform a rename in the
           ;; below destructure.
           {session-info :session-info token-info :extra-info} ret
           {:keys [serialized-extra-info]} token-info]
       (if session-info
         {:session-info session-info
          :token-info
          (assoc token-info :extra-info
                 (when serialized-extra-info
                   (au/<? (common/<serialized-value->value
                            {:<request-schema (za/make-schema-requester
                                                (:talk2-client zc))
                             :reader-schema extra-info-schema
                             :serialized-value serialized-extra-info
                             :storage (:storage zc)}))))}
         false)))))

(defn <create-actor!
   "Returns the actor-id of the created actor."
  ([zc identifier]
   (<create-actor! zc identifier nil))
  ([zc identifier actor-id]
   (when-not (string? identifier)
     (throw (ex-info (str "`identifier` must be a string. Got `"
                          (or identifier "nil") "`.")
                     (u/sym-map identifier))))
   (let [arg {:authenticator-name shared/authenticator-name
              :return-value-schema schemas/actor-id-schema
              :update-info-schema shared/create-actor-info-schema
              :update-info (u/sym-map identifier actor-id)
              :update-type :create-actor
              :zc zc}]
     (za/<client-update-authenticator-state arg))))

(defn <add-identifier!
  "Works on current actor. Must be logged in. Returns a boolean success value."
  [zc identifier]
   (when-not (string? identifier)
     (throw (ex-info (str "`identifier` must be a string. Got `"
                          (or identifier "nil") "`.")
                     (u/sym-map identifier))))
  (when-not (zc/logged-in? zc)
    (throw (ex-info "Must be logged in to call `<add-identifier!`"
                    (u/sym-map identifier))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :update-info-schema shared/identifier-schema
             :update-info identifier
             :update-type :add-identifier
             :zc zc}]
    (za/<client-update-authenticator-state arg)))

(defn <remove-identifier!
  "Works on current actor. Must be logged in. Returns a boolean success value."
  [zc identifier]
  (when-not (string? identifier)
    (throw (ex-info (str "`identifier` must be a string. Got `"
                         (or identifier "nil") "`.")
                    (u/sym-map identifier))))
  (when-not (zc/logged-in? zc)
    (throw (ex-info "Must be logged in to call `<remove-identifier!`"
                    (u/sym-map identifier))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :update-info-schema shared/identifier-schema
             :update-info identifier
             :update-type :remove-identifier
             :zc zc}]
    (za/<client-update-authenticator-state arg)))

(defn <log-out! [zc]
  (za/<client-log-out zc))

(defn <resume-session! [zc session-token]
  (au/go
    (let [ret (au/<? (za/<client-resume-session zc session-token))]
      (or ret false))))