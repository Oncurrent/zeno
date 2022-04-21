(ns unit.crdt-server-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.server :as crdt-server]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest ^:this test-<get-txs-since
  (au/test-async
   1000
   (ca/go
     (let [storage (storage/make-storage)
           branch "TEST-BRANCH"
           branch-log-k (crdt-server/branch->branch-log-k branch)
           actor-id "ACTOR-1"
           tx1-info {:actor-id actor-id
                     :tx-id "TX1"}
           tx2-info {:actor-id actor-id
                     :tx-id "TX2"}
           tx3-info {:actor-id actor-id
                     :tx-id "TX3"}]
       (is (= [] (au/<? (crdt-server/<get-txs-since
                         {:storage storage
                          :branch branch
                          :last-tx-id nil}))))
       (is (= [] (au/<? (crdt-server/<get-txs-since
                         {:storage storage
                          :branch branch
                          :last-tx-id "INVALID-TX-ID"}))))
       ;; Add tx-ids but no corresponding tx-infos
       (au/<? (storage/<swap! storage branch-log-k
                              shared/segmented-log-schema
                              (fn [old-log]
                                (update old-log :tx-ids concat
                                        ["TX1" "TX2" "TX3"]))))
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"tx-info for tx-id `TX1` was not found"
            (au/<? (crdt-server/<get-txs-since
                    {:storage storage
                     :branch branch
                     :last-tx-id nil}))))
       ;; Add the tx-infos
       (au/<? (crdt-server/<store-tx-infos
               {:serializable-tx-infos [tx1-info tx2-info tx3-info]
                :storage storage}))
       (is (= [tx1-info tx2-info tx3-info] (au/<? (crdt-server/<get-txs-since
                                                   {:storage storage
                                                    :branch branch
                                                    :last-tx-id nil}))))
       (is (= [tx2-info tx3-info] (au/<? (crdt-server/<get-txs-since
                                          {:storage storage
                                           :branch branch
                                           :last-tx-id "TX1"}))))
       (is (= [tx3-info] (au/<? (crdt-server/<get-txs-since
                                 {:storage storage
                                  :branch branch
                                  :last-tx-id "TX2"}))))
       (is (= [] (au/<? (crdt-server/<get-txs-since
                         {:storage storage
                          :branch branch
                          :last-tx-id "TX3"}))))))))
