(ns unit.server-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.server :as server]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (clojure.lang ExceptionInfo)))

(defn <get-member-urls [*member-urls]
  (au/go
    @*member-urls))

(defn <publish-member-urls [*member-urls urls]
  (au/go
    (reset! *member-urls urls)))

(deftest test-cluster-membership
  (au/test-async
   60000
   (ca/go
     (let [storage (storage/make-storage)
           *member-urls (atom nil)
           config {:<get-published-member-urls
                   (partial <get-member-urls *member-urls)

                   :<publish-member-urls
                   (partial <publish-member-urls *member-urls)

                   :storage storage
                   :ws-url "wss://198.51.100.10:443"}
           server1 (server/zeno-server config)
           server2 (server/zeno-server
                    (assoc config :ws-url "wss://198.51.100.70:443"))]
       (ca/<! (ca/timeout 20000))
       (is (= :foo storage))
       (server/stop! server1)
       (server/stop! server2)))))
