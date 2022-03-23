(ns com.oncurrent.zeno.authenticators.magic-token.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.authenticators.magic-token.shared :as shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.client.impl :as cimpl]
   [com.oncurrent.zeno.client.authentication :as za]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <request-magic-token!
  "Returns a boolean success value."
  [zc {:keys [identifier mins-valid number-of-uses extra-info extra-info-schema
              params params-schema]
       :as request-magic-token-info}]
  ;; We use regular `go` here instead of `au/go` because callers don't want to
  ;; wait for the server side handler which is where e.g. emails are sent, but
  ;; we still want to catch errors. Thus, we use our own try/catch with error
  ;; logging instead of `au/go`'s try/catch which puts the error on the
  ;; returned channel.
  (ca/go
   (try
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
    (when (and params (not params-schema))
      (throw (ex-info "params was provided with no params-schema"
                      (u/sym-map extra-info))))
    (when (and params-schema (not (l/schema? params-schema)))
      (throw (ex-info "params-schema was provided but is not a Lancaster schema."
                      (u/sym-map params-schema))))
    (let [request-magic-token-info*
          (cond-> request-magic-token-info
            (and extra-info extra-info-schema)
            (assoc :serialized-extra-info
                   (au/<? (storage/<value->serialized-value
                           (:storage zc) extra-info-schema extra-info)))
            (and params params-schema)
            (assoc :serialized-params
                   (au/<? (storage/<value->serialized-value
                           (:storage zc) params-schema params))))
          arg {:authenticator-name shared/authenticator-name
               :return-value-schema l/boolean-schema
               :update-info-schema shared/request-magic-token-info-schema
               :update-info request-magic-token-info*
               :update-type :request-magic-token
               :zc zc}]
      (au/<? (za/<client-update-authenticator-state arg))
      true)
    (catch Exception e
      (log/error (u/ex-msg-and-stacktrace e))))))

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
                :login-info-schema shared/magic-token-schema
                :login-ret-extra-info-schema shared/magic-token-info-schema
                :zc zc}
           ret (au/<? (za/<client-log-in arg))
           ;; Note that the plubming can return extra-info which has a naming
           ;; collision with this plugin's extra-info. In this case we use the
           ;; plumbing's extra-info to return the token-info which includes
           ;; our plugin's serialized-extra-info, so we perform a rename in the
           ;; below destructure.
           {login-session-info :login-session-info token-info :extra-info} ret
           {:keys [serialized-extra-info]} token-info]
       (if login-session-info
         {:login-session-info login-session-info
          :token-info
          (assoc token-info :extra-info
                 (when serialized-extra-info
                   (au/<? (common/<serialized-value->value
                           {:<request-schema (cimpl/make-schema-requester
                                              (:Talk2-client zc))
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

(defn <resume-login-session! [zc login-session-token]
  (au/go
    (let [ret (au/<? (za/<client-resume-login-session
                      (u/sym-map zc login-session-token)))]
      (or ret false))))
