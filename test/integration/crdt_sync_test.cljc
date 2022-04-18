(ns integration.crdt-sync-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :refer [deftest is]]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.state-providers.crdt :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.client :as crdt-client]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]
   [test-common :as c]))

;;;; IMPORTANT!!!
;;;; You must start the integration test server for these tests to work.
;;;; $ bin/run-test-server

;; TODO: Replace this with `c/->zc`
(defn make-zc
  ([]
   (make-zc {}))
  ([{:keys [client-name env-name source-env-name]}]
   (let [crdt-sp (crdt-client/->state-provider
                  #::crdt{:authorizer nil ; TODO: Fill this in
                          :schema c/crdt-schema})
         config #:zeno{:client-name client-name
                       :env-name env-name
                       :get-server-base-url (constantly "ws://localhost:8080")
                       :root->state-provider {:zeno/crdt crdt-sp}
                       :source-env-name source-env-name}]
     (zc/->zeno-client config))))

(deftest ^:this test-sp-comms
  (au/test-async
   15000
   (ca/go
     (let [zc (make-zc)]
       (try
         (is (= :foo :bar))
         (ca/<! (ca/timeout 3000))
         (is (= :done :now))
         (catch #?(:clj Exception :cljs js/Error) e
           (log/error (u/ex-msg-and-stacktrace e))
           (is (= :threw :but-should-not-have)))
         (finally
           (zc/stop! zc)))))))
