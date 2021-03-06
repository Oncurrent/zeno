(ns unit.crdt-server-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.authorizers.affirmative-authorizer.server :as auth]
   [com.oncurrent.zeno.bulk-storage :as bulk-storage]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as common]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.state-providers.crdt.server :as server]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [kaocha.repl]
   [taoensso.timbre :as log]
   [test-common :as tc])
  (:import
   (clojure.lang ExceptionInfo)))

(defn <make-ser-tx-info
  [{:keys [actor-id client-id cmds root schema storage tx-id]}]
  (au/go
    (let [{:keys [crdt-ops updated-paths]} (commands/process-cmds
                                            (u/sym-map cmds schema root))
          ser-ops (au/<? (common/<crdt-ops->serializable-crdt-ops
                          (u/sym-map crdt-ops schema storage)))]
      {:actor-id actor-id
       :client-id client-id
       :crdt-ops ser-ops
       :sys-time-ms 1653368731993
       :tx-id tx-id
       :updated-paths updated-paths})))

(defn <log-tx!
  [{:keys [actor-id client-id cmds make-tx-id root schema]
    :as arg}]
  (au/go
    (let [tx-id (make-tx-id)
          ser-tx-info (au/<? (<make-ser-tx-info (assoc arg :tx-id tx-id)))]
      (au/<? (server/<log-producer-tx-batch!
              (assoc arg :serializable-tx-infos [ser-tx-info]))))))

(comment
 (kaocha.repl/run #'test-sync {:capture-output? false :color? false}))
(deftest test-sync
  (au/test-async
   3000
   (au/go
     (let [bulk-storage (bulk-storage/->mem-bulk-storage {:server-port 9997})]
       (try
         (let [*branch->crdt-info (atom {})
               branch "test-branch"
               branch-log-k (server/->branch-log-k (u/sym-map branch))
               schema tc/crdt-schema
               storage (storage/make-storage)
               authorizer (auth/->authorizer)
               root :my-root
               actor-id "actor-1"
               client-id "client-1"
               snapshot-interval 2
               *tx-id (atom 0)
               make-tx-id #(str "tx-" (swap! *tx-id inc))
               arg (u/sym-map *branch->crdt-info actor-id authorizer branch
                              bulk-storage client-id make-tx-id root schema
                              snapshot-interval storage)
               book  {:title "Tall Tales"
                      :nums [3 6 9]}
               book-id "book-id"
               cmds-1 [{:zeno/arg book
                        :zeno/op :zeno/set
                        :zeno/path [root :books book-id]}]
               _ (is (= true (au/<? (<log-tx! (assoc arg :cmds cmds-1)))))
               _ (is (= {:actor-id-to-log-info
                         {"__BRANCH_MAIN__"
                          {:branch-log-tx-indices-since-snapshot [0]
                           :snapshot-tx-index -1
                           :snapshot-txs-hash 0}}
                         :branch-tx-ids ["tx-1"]}
                        (au/<? (storage/<get storage branch-log-k
                                             shared/branch-log-info-schema))))
               gcsi-ret-1 (au/<? (server/<get-consumer-sync-info arg))
               _ (is (= {:snapshot-tx-index -1
                         :snapshot-url nil
                         :tx-ids-since-snapshot ["tx-1"]}
                        gcsi-ret-1))
               the-id "xyz-123"
               cmds-2 [{:zeno/arg the-id
                        :zeno/op :zeno/set
                        :zeno/path [root :the-id]}]
               _ (is (= true (au/<? (<log-tx! (assoc arg :cmds cmds-2)))))
               gcsi-ret-2 (au/<? (server/<get-consumer-sync-info arg))
               _ (is (= 1 (:snapshot-tx-index gcsi-ret-2)))
               _ (is (= [] (:tx-ids-since-snapshot gcsi-ret-2)))
               snapshot (au/<? (common/<get-snapshot-from-url
                                (assoc arg :url (:snapshot-url gcsi-ret-2))))
               _ (is (= [book-id] (-> snapshot :crdt :children :books
                                      :children keys)))])
         (finally
           (bulk-storage/<stop-server! bulk-storage)))))))
