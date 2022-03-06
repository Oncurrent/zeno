(ns integration.authentication-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.authenticators.identifier-secret.client :as isa]
   [oncurrent.zeno.client :as zc]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

(defn make-identifier []
  (str "user-" (u/compact-random-uuid) "@email.com"))

(deftest ^:this test-identity-secret-authenticator
  (au/test-async
   10000
   (ca/go
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
