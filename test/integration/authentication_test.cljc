(ns integration.authentication-test
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   #?(:clj [clojure.java.io :as io])
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   #?(:clj kaocha.repl)
   [com.oncurrent.zeno.authenticators.identifier-secret.client :as isa]
   [com.oncurrent.zeno.authenticators.magic-token.client :as mta]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(comment (kaocha.repl/run 'integration.authentication-test))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-zc
  ([] (make-zc nil))
  ([extra-config]
   (let [config (merge {:crdt-branch "integration-test"
                        :get-server-url (constantly "ws://localhost:8080/client")}
                       extra-config)
         zc (zc/zeno-client config)
         clean-up! #(zc/stop! zc)]
     (u/sym-map zc clean-up!))))

(def ex #?(:clj Exception :cljs js/Error))

(defn catcher [e]
  (log/error (u/ex-msg-and-stacktrace e))
  (is (= :threw :but-should-not-have)))

(defn make-identifier []
  (str "user-" (u/compact-random-uuid) "@email.com"))

(comment (kaocha.repl/run 'integration.authentication-test/test-identifier-secret-authenticator))
(deftest test-identifier-secret-authenticator
  (au/test-async
   10000
   (au/go
     (let [{:keys [zc clean-up!]} (make-zc)]
       (try
         (let [identifier (make-identifier)
               secret "a secret that has some spaces CAPS 132412 !!#$@|_.*<>"
               created-actor-id (au/<? (isa/<create-actor!
                                        zc identifier secret))
               _ (is (string? created-actor-id))
               login-ret (au/<? (isa/<log-in! zc identifier secret))
               _ (is (= created-actor-id (:actor-id login-ret)))
               identifier2 (make-identifier)
               _ (is (= true (au/<? (isa/<add-identifier! zc identifier2))))
               _ (is (= true (au/<? (isa/<log-out! zc))))
               login-ret2 (au/<? (isa/<log-in! zc identifier2 secret))
               _ (is (= created-actor-id (:actor-id login-ret2)))
               _ (is (= true (au/<? (isa/<remove-identifier! zc identifier))))
               _ (is (= true (au/<? (isa/<log-out! zc))))
               _ (is (= false (au/<? (isa/<log-in! zc identifier secret))))
               login-ret3 (au/<? (isa/<log-in! zc identifier2 secret))
               _ (is (= created-actor-id (:actor-id login-ret3)))
               new-secret "Robert`);DROP TABLE Students;--"
               _ (is (= true (au/<? (isa/<set-secret! zc secret new-secret))))
               _ (is (= true (au/<? (isa/<log-out! zc))))
               _ (is (= false (au/<? (isa/<log-in! zc identifier2 secret))))
               login-ret4 (au/<? (isa/<log-in! zc identifier2 new-secret))
               _ (is (= created-actor-id (:actor-id login-ret4)))
               {:keys [login-session-token]} login-ret4
               {zc2 :zc clean-up2! :clean-up!} (make-zc)
               rs-ret (au/<? (isa/<resume-login-session!
                              zc2 login-session-token))]
           (is (= login-session-token (:login-session-token rs-ret)))
           (is (= created-actor-id (:actor-id rs-ret)))
           (is (= false (au/<? (isa/<resume-login-session!
                                zc2 "an-invalid-token"))))
           (clean-up2!))
         (catch ex e (catcher e))
         (finally (clean-up!)))))))

;; TODO: Test adding an identifier that already exists, etc.

(defn magic-token-extras []
  (let [extra-info-file (java.io.File/createTempFile "email" ".txt")
        extra-info (.getAbsolutePath extra-info-file)
        extra-info-schema l/string-schema
        count-lines (fn [filepath] (with-open [rdr (io/reader filepath)]
                                     (count (line-seq rdr))))
        last-line (fn [filepath] (with-open [rdr (io/reader filepath)]
                                   (edn/read-string (last (line-seq rdr)))))
        clean-up! #(.delete ^java.io.File extra-info-file)]
    (is (= 0 (count-lines extra-info)))
    (u/sym-map extra-info-file extra-info extra-info-schema
               count-lines last-line clean-up!)))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-expiration))
(deftest test-magic-token-authenticator-expiration
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             _ (au/<? (mta/<create-actor! zc identifier))
             _ (au/<? (mta/<request-magic-token!
                       zc (-> (u/sym-map identifier
                                         extra-info
                                         extra-info-schema)
                              (assoc :mins-valid 0))))
             _ (is (= 1 (count-lines extra-info)))
             token (-> extra-info last-line :token)]
         (is (= false (au/<? (mta/<redeem-magic-token!
                              zc token extra-info-schema))))
         (is (= 1 (count-lines extra-info))))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-number-of-uses))
(deftest test-magic-token-authenticator-number-of-uses
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             created-actor-id (au/<? (mta/<create-actor! zc identifier))
             _ (au/<? (mta/<request-magic-token!
                       zc (-> (u/sym-map identifier
                                         extra-info
                                         extra-info-schema)
                              (assoc :number-of-uses 1))))
             _ (is (= 1 (count-lines extra-info)))
             token (-> extra-info last-line :token)
             redeem-ret (au/<? (mta/<redeem-magic-token!
                                zc token extra-info-schema))
             extra-info* (-> redeem-ret :token-info :extra-info)
             actor-id (-> redeem-ret :login-session-info :actor-id)]
         (is (= 2 (count-lines extra-info*)))
         (is (= created-actor-id
                actor-id
                (-> extra-info* last-line :token-info :actor-id)))
         (is (= true (au/<? (mta/<log-out! zc))))
         (is (= false (au/<? (mta/<redeem-magic-token!
                              zc token extra-info-schema))))
         (is (= 2 (count-lines extra-info))))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(defn test-magic-token-authenticator-basic-happy-path*
  [{:keys [create-actor?]}]
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             created-actor-id (when create-actor?
                                (au/<? (mta/<create-actor! zc identifier)))
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc
                     (u/sym-map identifier extra-info extra-info-schema)
                     :mins-valid :unlimited
                     :number-of-uses :unlimited)))
             _ (is (= 1 (count-lines extra-info)))
             token (-> extra-info last-line :token)
             created-actor-id (or created-actor-id
                                  (-> extra-info last-line
                                      :token-info :actor-id))
             redeem-ret (au/<? (mta/<redeem-magic-token!
                                zc token extra-info-schema))
             extra-info* (-> redeem-ret :token-info :extra-info)
             actor-id (-> redeem-ret :login-session-info :actor-id)]
         (is (= 2 (count-lines extra-info*)))
         (is (= created-actor-id
                actor-id
                (-> extra-info last-line :token-info :actor-id)
                (-> extra-info* last-line :token-info :actor-id)))
         (is (= true (au/<? (mta/<log-out! zc)))))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-basic-happy-path))
(deftest test-magic-token-authenticator-basic-happy-path
  (test-magic-token-authenticator-basic-happy-path* {:create-actor? true})
  ;; Requesting a magic token creates an actor for you.
  (test-magic-token-authenticator-basic-happy-path* {:create-actor? false}))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-second-identifier))
(deftest test-magic-token-authenticator-second-identifier
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             created-actor-id (au/<? (mta/<create-actor! zc identifier))
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc
                     (u/sym-map identifier extra-info extra-info-schema)
                     :number-of-uses 2)))
             token (-> extra-info last-line :token)
             _ (au/<? (mta/<redeem-magic-token!
                       zc token extra-info-schema))
             ;; Can make second identifier.
             identifier2 (make-identifier)
             _ (is (= true (au/<? (mta/<add-identifier! zc identifier2))))
             _ (is (= true (au/<? (mta/<log-out! zc))))
             ;; Old token still works.
             redeem-ret* (au/<? (mta/<redeem-magic-token!
                                 zc token extra-info-schema))
             extra-info* (-> redeem-ret* :token-info :extra-info)
             actor-id* (-> redeem-ret* :login-session-info :actor-id)
             _ (is (= 3 (count-lines extra-info*)))
             _ (is (= created-actor-id (-> extra-info* last-line
                                           :token-info :actor-id)))
             _ (is (= created-actor-id actor-id*))
             _ (au/<? (mta/<log-out! zc))
             ;; Second identifier returns same actor id
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc
                     (u/sym-map identifier extra-info extra-info-schema)
                     :number-of-uses 1)))
             _ (is (= 4 (count-lines extra-info)))
             token2 (-> extra-info last-line :token)
             redeem-ret2 (au/<? (mta/<redeem-magic-token!
                                 zc token2 extra-info-schema))
             extra-info2* (-> redeem-ret2 :token-info :extra-info)
             actor-id2 (-> redeem-ret2 :login-session-info :actor-id)]
             (is (= created-actor-id actor-id2))
             (au/<? (mta/<log-out! zc)))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-remove-identifier))
(deftest test-magic-token-authenticator-remove-identifier
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             identifier2 (make-identifier)
             created-actor-id (au/<? (mta/<create-actor! zc identifier))
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc
                     (u/sym-map identifier extra-info extra-info-schema)
                     :number-of-uses 3)))
             token (-> extra-info last-line :token)
             _ (au/<? (mta/<redeem-magic-token!
                       zc token extra-info-schema))
             _ (au/<? (mta/<add-identifier! zc identifier2))
             _ (is (= true (au/<? (mta/<remove-identifier! zc identifier2))))
             _ (au/<? (mta/<log-out! zc))
             ;; Removing identifier2 which didn't request the token doesn't
             ;; effect the token requested by identifier.
             redeem-ret (au/<? (mta/<redeem-magic-token!
                                zc token extra-info-schema))
             extra-info* (-> redeem-ret :token-info :extra-info)
             actor-id (-> redeem-ret :login-session-info :actor-id)
             _ (is (= 3 (count-lines extra-info*)))
             _ (is (= created-actor-id (-> extra-info* last-line
                                           :token-info :actor-id)))
             _ (is (= created-actor-id actor-id))
             ;; Removing identifier invalidates the token.
             _ (is (= true (au/<? (mta/<remove-identifier! zc identifier))))
             _ (au/<? (mta/<log-out! zc))
             _ (is (= false (au/<? (mta/<redeem-magic-token!
                                    zc token extra-info-schema))))
             _ (is (= 3 (count-lines extra-info)))
             ;; Creating a new actor with that same identifier does not
             ;; allow the previously identified actor to now have access to
             ;; the new actor's stuff.
             new-actor-id (au/<? (mta/<create-actor! zc identifier))
             _ (is (= false (au/<? (mta/<redeem-magic-token!
                                    zc token extra-info-schema))))
             _ (is (= 3 (count-lines extra-info)))
             ;; But they can request their own and login, then remove
             ;; themselves.
             _ (au/<?
                (mta/<request-magic-token!
                 zc (u/sym-map identifier extra-info extra-info-schema)))
             token-new (-> extra-info last-line :token)
             redeem-ret-new (au/<? (mta/<redeem-magic-token!
                                    zc token-new extra-info-schema))
             extra-info-new (-> redeem-ret-new :token-info :extra-info)
             actor-id-new (-> redeem-ret-new :login-session-info :actor-id)
             _ (is (= 5 (count-lines extra-info-new)))
             _ (is (= new-actor-id (-> extra-info-new last-line
                                       :token-info :actor-id)))
             _ (is (= new-actor-id actor-id-new))
             _ (is (= true (au/<? (mta/<remove-identifier! zc identifier))))
             ;; But if I create an actor using the same original actor-id
             ;; with the same identifier the token will work and we can use
             ;; that last remaining use of the token.
             original-actor-id (au/<? (mta/<create-actor! zc identifier
                                                          created-actor-id))
             redeem-ret* (au/<? (mta/<redeem-magic-token!
                                 zc token extra-info-schema))
             extra-info** (-> redeem-ret* :token-info :extra-info)
             actor-id* (-> redeem-ret* :login-session-info :actor-id)
             _ (is (= 6 (count-lines extra-info**)))
             _ (is (= created-actor-id (-> extra-info** last-line
                                           :token-info :actor-id)))
             _ (is (= created-actor-id original-actor-id actor-id*))
             _ (is (= true (au/<? (mta/<log-out! zc))))])
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-resume-login-session))
(deftest test-magic-token-authenticator-resume-login-session
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier (make-identifier)
             created-actor-id (au/<? (mta/<create-actor! zc identifier))
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc
                     (u/sym-map identifier extra-info extra-info-schema)
                     :number-of-uses 1)))
             token (-> extra-info last-line :token)
             redeem-ret (au/<? (mta/<redeem-magic-token!
                                zc token extra-info-schema))
             actor-id (-> redeem-ret :login-session-info :actor-id)
             login-session-token (-> redeem-ret
                                     :login-session-info
                                     :login-session-token)
             {zc2 :zc clean-up2! :clean-up!} (make-zc)
             rs-ret (au/<? (mta/<resume-login-session!
                            zc2 login-session-token))
             actor-id* (:actor-id rs-ret)]
         (is (= created-actor-id actor-id actor-id*))
         (is (= false (au/<? (mta/<resume-login-session!
                              zc2 "an invalid-token"))))
         (is (= true (au/<? (mta/<log-out! zc))))
         ; TODO
         #_(is (= true (au/<? (mta/<log-out! zc2))))
         (clean-up2!))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-differing-actor-ids))
(deftest test-magic-token-authenticator-differing-actor-ids
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)]
      (try
       (let [identifier1 (make-identifier)
             actor1 (au/<? (mta/<create-actor! zc identifier1))
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc (u/sym-map extra-info extra-info-schema)
                           :identifier identifier1)))
             request1-handle (-> extra-info last-line)
             token1 (:token request1-handle)
             redeem1-ret (au/<? (mta/<redeem-magic-token!
                                 zc token1 extra-info-schema))
             redeem1-handle (-> redeem1-ret :token-info :extra-info last-line)
             identifier2 (make-identifier)
             _ (au/<?
                (mta/<request-magic-token!
                 zc (assoc (u/sym-map extra-info extra-info-schema)
                           :identifier identifier2)))
             request2-handle (-> extra-info last-line)
             actor2 (-> request2-handle :token-info :actor-id)
             token2 (:token request2-handle)
             _ (au/<? (mta/<log-out! zc))
             redeem2-ret (au/<? (mta/<redeem-magic-token!
                                 zc token2 extra-info-schema))
             redeem2-handle (-> redeem2-ret :token-info :extra-info last-line)]
         (au/<? (mta/<log-out! zc))
         (is (= identifier1 (-> request1-handle :token-info :identifier)))
         (is (= identifier1 (-> redeem1-handle :token-info :identifier)))
         (is (= identifier1 (-> redeem1-ret :token-info :identifier)))
         ;; Notice this next one, no one was logged in when the request
         ;; happened. It was a request in order to log in.
         (is (= nil (-> request1-handle :actor-id)))
         (is (= nil (-> redeem1-handle :token-info :requestor-id)))
         (is (= actor1 (-> request1-handle :token-info :actor-id)))
         (is (= actor1 (-> redeem1-handle :token-info :actor-id)))
         (is (= actor1 (-> redeem1-ret :token-info :actor-id)))
         (is (= actor1 (-> redeem1-ret :login-session-info :actor-id)))
         (is (= identifier2 (-> request2-handle :token-info :identifier)))
         (is (= identifier2 (-> redeem2-handle :token-info :identifier)))
         (is (= identifier2 (-> redeem2-ret :token-info :identifier)))
         ;; Notice this next one, actor1 requested a token for actor2, perhaps
         ;; inviting them to use the app.
         (is (= actor1 (-> request2-handle :actor-id)))
         (is (= actor1 (-> redeem2-handle :token-info :requestor-id)))
         (is (= actor2 (-> redeem2-handle :token-info :actor-id)))
         (is (= actor2 (-> redeem2-ret :token-info :actor-id)))
         (is (= actor2 (-> redeem2-ret :login-session-info :actor-id))))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))

(comment (kaocha.repl/run 'integration.authentication-test/test-magic-token-authenticator-params))
(deftest test-magic-token-authenticator-params
  (au/test-async
   10000
   (au/go
    (let [{zc :zc clean-up-zc! :clean-up!} (make-zc)
          {:keys [extra-info-file extra-info extra-info-schema
                  count-lines last-line]
           clean-up-extras! :clean-up!} (magic-token-extras)
          params "My param."
          params-schema l/string-schema
          identifier (make-identifier)]
      (try
       (au/<?
        (mta/<request-magic-token!
         zc (u/sym-map identifier
                       extra-info extra-info-schema
                       params params-schema)))
       (is (= params (-> extra-info last-line :params)))
       (catch ex e (catcher e))
       (finally (clean-up-zc!) (clean-up-extras!)))))))
