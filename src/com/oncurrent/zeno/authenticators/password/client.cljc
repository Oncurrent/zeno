(ns com.oncurrent.zeno.authenticators.password.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client.authenticator-impl :as za]
   [com.oncurrent.zeno.authenticators.password.shared :as shared]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <create-actor!
  "Returns the actor-id of the created actor."
  ([zc identifier secret]
   (<create-actor! zc identifier secret nil))
  ([zc identifier secret actor-id]
   (let [arg {:authenticator-name shared/authenticator-name
              :return-value-schema schemas/actor-id-schema
              :update-info-schema shared/create-actor-info-schema
              :update-info (u/sym-map identifier secret actor-id)
              :update-type :create-actor
              :zc zc}]
     (za/<client-update-authenticator-state arg))))

(defn <add-identifier!
  "Works on current actor. Must be logged in. Returns a boolean success value."
  [zc identifier]
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

(defn <set-secret!
  "Works on current actor. Must be logged in. Returns a boolean success value."
  [zc old-secret new-secret]
  (when-not (zc/logged-in? zc)
    (throw (ex-info "Must be logged in to call `<set-secret!`" {})))
  (when-not (string? old-secret)
    (throw (ex-info (str "`old-secret` must be a string. Got `"
                         (or old-secret "nil") "`.")
                    (u/sym-map old-secret))))
  (when-not (string? new-secret)
    (throw (ex-info (str "`new-secret` must be a string. Got `"
                         (or new-secret "nil") "`.")
                    (u/sym-map new-secret))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :update-info-schema shared/set-secret-info-schema
             :update-info (u/sym-map old-secret new-secret)
             :update-type :set-secret
             :zc zc}]
    (za/<client-update-authenticator-state arg)))

(defn <log-in! [zc identifier secret]
  (au/go
    (when-not (string? identifier)
      (throw (ex-info (str "`identifier` must be a string. Got `"
                           (or identifier "nil") "`.")
                      (u/sym-map identifier))))
    (when-not (string? secret)
      (throw (ex-info (str "`secret` must be a string. Got `"
                           (or secret "nil") "`.")
                      (u/sym-map secret identifier))))
    (let [arg {:authenticator-name shared/authenticator-name
               :login-info (u/sym-map identifier secret)
               :login-info-schema shared/login-info-schema
               :zc zc}
          ret (au/<? (za/<client-log-in arg))]
      (or (:login-session-info ret)
          false))))

(defn <identifier-taken? [zc identifier]
  (when-not (string? identifier)
    (throw (ex-info (str "`identifier` must be a string. Got `"
                         (or identifier "nil") "`.")
                    (u/sym-map identifier))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :get-info-schema shared/identifier-schema
             :get-info identifier
             :get-type :is-identifier-taken
             :zc zc}]
    (za/<client-get-authenticator-state arg)))

(defn <log-out! [zc]
  (za/<client-log-out zc))

(defn <resume-login-session! [zc login-session-token]
  (au/go
    (let [ret (au/<? (za/<client-resume-login-session
                      (u/sym-map login-session-token zc)))]
      (or ret false))))
