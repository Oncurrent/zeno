(ns integration.authentication-test
  (:require
   [clojure.core.async :as ca]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   kaocha.repl
   [com.oncurrent.zeno.authenticators.identifier-secret.client :as isa]
   [com.oncurrent.zeno.authenticators.magic-token.client :as mta]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-identifier []
  (str "user-" (u/compact-random-uuid) "@email.com"))

(comment
  (kaocha.repl/run
    'integration.authentication-test/test-identity-secret-authenticator)
  )
(deftest test-identity-secret-authenticator
  (au/test-async
   10000
   (au/go
     (let [config {:branch "integration-test"
                   :get-server-url (constantly "ws://localhost:8080/client")}
           zc (zc/zeno-client config)]
       (try
         (let [identifier (make-identifier)
               secret "a secret that has some spaces CAPS 132412 !!#$@|_.*<>"
               created-actor-id (au/<? (isa/<create-actor!
                                        zc identifier secret))
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
               {:keys [session-token]} login-ret4
               zc2 (zc/zeno-client config)
               rs-ret (au/<? (isa/<resume-session! zc2 session-token))]
           (is (= session-token (:session-token rs-ret)))
           (is (= created-actor-id (:actor-id rs-ret)))
           (is (= false (au/<? (isa/<resume-session! zc2 "an-invalid-token"))))
           (zc/shutdown! zc2))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/shutdown! zc)))))))

;; TODO: Test adding an identifier that already exists, etc.

(comment
  (kaocha.repl/run
    'integration.authentication-test/test-magic-token-authenticator-expiration)
  )
(deftest test-magic-token-authenticator-expiration
  (au/test-async
    10000
    (au/go
      (let [config {:branch "integration-test"
                    :get-server-url (constantly "ws://localhost:8080/client")}
            zc (zc/zeno-client config)
            extra-info* (java.io.File/createTempFile "email" ".txt")
            extra-info (.getAbsolutePath extra-info*)
            extra-info-schema l/string-schema
            count-lines (fn [filepath] (with-open [rdr (io/reader filepath)]
                                         (count (line-seq rdr))))
            last-line (fn [filepath] (with-open [rdr (io/reader filepath)]
                                       (last (line-seq rdr))))]
        (try
          (let [_ (is (= 0 (count-lines extra-info)))
                identifier (make-identifier)
                created-actor-id (au/<? (mta/<create-actor! zc identifier))

                ;; Test token expiration.
                _ (au/<?
                    (mta/<send-magic-token!
                      zc (assoc
                           (u/sym-map identifier extra-info extra-info-schema)
                           :mins-valid 0)))
                _ (is (= 1 (count-lines extra-info)))
                expired-token (last-line extra-info)
                _ (is (= false (au/<? (mta/<redeem-magic-token!
                                        zc expired-token extra-info-schema))))
                _ (is (= 1 (count-lines extra-info)))

                ;; Test number of uses.
                _ (au/<?
                    (mta/<send-magic-token!
                      zc (assoc
                           (u/sym-map identifier extra-info extra-info-schema)
                           :number-of-uses 1)))
                _ (is (= 2 (count-lines extra-info)))
                used-token (last-line extra-info)
                used-redeem-ret (au/<? (mta/<redeem-magic-token!
                                         zc used-token extra-info-schema))]
            (is (= 3 (count-lines extra-info)))
            (is (= "did-action!" (last-line extra-info)))
            (is (= created-actor-id
                   (-> used-redeem-ret :session-info :actor-id)))
            (is (= true (au/<? (mta/<log-out! zc))))
            (is (= false (au/<? (mta/<redeem-magic-token!
                                  zc used-token extra-info-schema))))
            (is (= 3 (count-lines extra-info))))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error (u/ex-msg-and-stacktrace e))
            (is (= :threw :but-should-not-have)))
          (finally
            (.delete ^java.io.File extra-info*)
            (zc/shutdown! zc)))))))

(comment
  (kaocha.repl/run
    'integration.authentication-test/test-magic-token-authenticator)
  )
(deftest test-magic-token-authenticator
  (au/test-async
    15000
    (au/go
      (let [config {:branch "integration-test"
                    :get-server-url (constantly "ws://localhost:8080/client")}
            zc (zc/zeno-client config)
            extra-info* (java.io.File/createTempFile "email" ".txt")
            extra-info (.getAbsolutePath extra-info*)
            extra-info-schema l/string-schema
            count-lines (fn [filepath] (with-open [rdr (io/reader filepath)]
                                         (count (line-seq rdr))))
            last-line (fn [filepath] (with-open [rdr (io/reader filepath)]
                                       (last (line-seq rdr))))]
        (try
          (let [_ (is (= 0 (count-lines extra-info)))
                identifier (make-identifier)
                created-actor-id (au/<? (mta/<create-actor! zc identifier))

                ;; Test standard send/redeem path.
                _ (au/<?
                    (mta/<send-magic-token!
                      zc (assoc
                           (u/sym-map identifier extra-info extra-info-schema)
                           :number-of-uses 3)))
                _ (is (= 1 (count-lines extra-info)))
                token (last-line extra-info)
                redeem-ret (au/<? (mta/<redeem-magic-token!
                                    zc token extra-info-schema))
                _ (is (= 2 (count-lines extra-info)))
                _ (is (= "did-action!" (last-line extra-info)))
                _ (is (= created-actor-id
                         (-> redeem-ret :session-info :actor-id)))
                _ (is (= true (au/<? (mta/<log-out! zc))))

                ;; Creating another identifier doesn't stop the token from
                ;; working.
                identifier2 (make-identifier)
                _ (is (= true (au/<? (mta/<add-identifier! zc identifier2))))
                redeem-ret* (au/<? (mta/<redeem-magic-token!
                                     zc token extra-info-schema))
                _ (is (= 3 (count-lines extra-info)))
                _ (is (= "did-action!" (last-line extra-info)))
                _ (is (= created-actor-id
                         (-> redeem-ret* :session-info :actor-id)))

                ;; Removing the identifier invalidates the token, note it has
                ;; one use left, hasn't expired, but still won't work.
                _ (is (= true (au/<? (mta/<remove-identifier! zc identifier))))
                _ (is (= true (au/<? (mta/<log-out! zc))))
                _ (is (= false (au/<? (mta/<redeem-magic-token!
                                        zc token extra-info-schema))))
                _ (is (= 3 (count-lines extra-info)))

                ;; Creating a new actor with that same identifier does not
                ;; allow the previously identified actor to now have access to
                ;; the new actor's stuff. Note the token still has one use left
                ;; and hasn't expired.
                new-actor-id (au/<? (mta/<create-actor! zc identifier))
                _ (is (= false (au/<? (mta/<redeem-magic-token!
                                        zc token extra-info-schema))))
                _ (is (= 3 (count-lines extra-info)))

                ;; But if I create an actor using the same original actor-id
                ;; with the same identifier the token will work and we can use
                ;; that last remaining use of the token.
                _ (is (= true (au/<? (mta/<remove-identifier! zc identifier))))
                original-actor-id (au/<? (mta/<create-actor! zc identifier
                                                             created-actor-id))
                redeem-ret** (au/<? (mta/<redeem-magic-token!
                                      zc token extra-info-schema))
                _ (is (= 4 (count-lines extra-info)))
                _ (is (= "did-action!" (last-line extra-info)))
                _ (is (= created-actor-id
                         original-actor-id
                         (-> redeem-ret** :session-info :actor-id)))
                _ (is (= true (au/<? (mta/<log-out! zc))))

                ;; identifier2 can get a token and the actor-id should match
                ;; that returned to identifier.
                _ (au/<?
                    (mta/<send-magic-token!
                      zc (assoc (u/sym-map extra-info extra-info-schema)
                                :identifier identifier2)))
                _ (is (= 5 (count-lines extra-info)))
                token2 (last-line extra-info)
                redeem-ret2 (au/<? (mta/<redeem-magic-token!
                                     zc token2 extra-info-schema))
                _ (is (= 6 (count-lines extra-info)))
                _ (is (= "did-action!" (last-line extra-info)))
                _ (is (= created-actor-id
                         (-> redeem-ret2 :session-info :actor-id)))

                ;; identifier2's token is now used up but the session-token
                ;; should still be good for resuming on a new client. We create
                ;; a new client since logging out would invalidate the session
                ;; token.
                zc2 (zc/zeno-client config)
                _ (is (= false (au/<? (mta/<redeem-magic-token!
                                        zc2 token2 extra-info-schema))))
                _ (is (= 6 (count-lines extra-info)))
                session-token (-> redeem-ret2 :session-info :session-token)
                rs-ret (au/<? (mta/<resume-session! zc2 session-token))]
            (is (= session-token (:session-token rs-ret)))
            (is (= created-actor-id (:actor-id rs-ret)))
            (is (= false (au/<? (mta/<resume-session! zc2 "an-invalid-token"))))
            (is (= true (au/<? (mta/<log-out! zc))))
            (is (= true (au/<? (mta/<log-out! zc2))))
            (zc/shutdown! zc2))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error (u/ex-msg-and-stacktrace e))
            (is (= :threw :but-should-not-have)))
          (finally
            (.delete ^java.io.File extra-info*)
            (zc/shutdown! zc)))))))
