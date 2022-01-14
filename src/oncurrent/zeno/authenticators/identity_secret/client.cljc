(ns oncurrent.zeno.authenticators.identifier-secret.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.authentication :as za]
   [oncurrent.zeno.authentication.plugins.identifier-secret.shared :as shared]
   [oncurrent.zeno.client :as zc]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <create-subject!
  ([zc identifier secret]
   (<create-subject zc identifier secret nil))
  ([zc identifier secret subject-id]
   "Returns the subject-id of the created subject."
   (let [arg {:authenticator-name shared/authenticator-name
              :return-value-schema schemas/subject-id-schema
              :update-info-schema shared/create-subject-info-schema
              :update-info (u/sym-map identifier secret subject-id)
              :update-type :create-subject
              :zc zc}]
     (za/<client-update-authenticator-state arg))))

(defn <add-identifier! [zc identifier]
  "Works on current subject. Must be logged in. Returns a boolean success value."
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

(defn <remove-identifier! [zc identifier]
  "Works on current subject. Must be logged in. Returns a boolean success value."
  (when-not (string? identifier)
    (throw (ex-info (str "`idenfifier` must be a string. Got `"
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

(defn <set-secret! [zc identifier old-secret new-secret]
  "Works on current subject. Must be logged in. Returns a boolean success value."
  (when-not (zc/logged-in? zc)
    (throw (ex-info "Must be logged in to call `<set-secret!`"
                    (u/sym-map identifier))))
  (when-not (string? identifier)
    (throw (ex-info (str "`idenfifier` must be a string. Got `"
                         (or identifier "nil") "`.")
                    (u/sym-map identifier))))
  (when-not (string? old-secret)
    (throw (ex-info (str "`old-secret` must be a string. Got `"
                         (or old-secret "nil") "`.")
                    (u/sym-map old-secret identifier))))
  (when-not (string? new-secret)
    (throw (ex-info (str "`new-secret` must be a string. Got `"
                         (or new-secret "nil") "`.")
                    (u/sym-map new-secret identifier))))
  (let [arg {:authenticator-name shared/authenticator-name
             :return-value-schema l/boolean-schema
             :update-info-schema shared/set-secret-info-schema
             :update-info (u/sym-map identifier old-secret new-secret)
             :update-type :set-secret
             :zc zc}]
    (za/<client-update-authenticator-state arg)))

(defn <log-in! [zc identifier secret]
  (au/go
    (when-not (string? identifier)
      (throw (ex-info (str "`idenfifier` must be a string. Got `"
                           (or identifier "nil") "`.")
                      (u/sym-map identifier))))
    (when-not (string? secret)
      (throw (ex-info (str "`secret` must be a string. Got `"
                           (or secret "nil") "`.")
                      (u/sym-map secret identifier))))
    (let [login-info
          arg {:authenticator-name shared/authenticator-name
               :login-info (u/sym-map identifier secret)
               :login-info-schema shared/login-info-schema
               :zc zc}
          ret (au/<? (za/<client-log-in arg))]
      (:session-info ret))))

(defn <log-out! [zc]
  (za/<client-log-out zc))

(defn <resume-session! [zc session-token]
  (za/<client-resume-session zc session-token))
