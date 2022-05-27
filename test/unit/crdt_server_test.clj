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
   [taoensso.timbre :as log]
   [test-common :as tc])
  (:import
   (clojure.lang ExceptionInfo)))

(defn <make-ser-tx-info
  [{:keys [actor-id client-id cmds root schema storage tx-id]}]
  (au/go
    (let [{:keys [ops update-infos]} (commands/process-cmds {:cmds cmds
                                                             :schema schema
                                                             :root root})
          ser-ops (au/<? (common/<crdt-ops->serializable-crdt-ops
                          (u/sym-map ops schema storage)))
          ser-update-infos (au/<?
                            (common/<update-infos->serializable-update-infos
                             (u/sym-map schema storage update-infos)))]
      {:actor-id actor-id
       :client-id client-id
       :crdt-ops ser-ops
       :sys-time-ms 1653368731993
       :tx-id tx-id
       :update-infos ser-update-infos})))

(deftest ^:this test-sync
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
               actor-id "actor-1"
               client-id "client-1"
               tx-id "abx7m2pxfphen96wybd71j1n2m"
               root :my-root
               book  {:title "Tall Tales"
                      :nums [3 6 9]}
               book-id "book-id"
               cmds [{:zeno/arg book
                      :zeno/op :zeno/set
                      :zeno/path [root :books book-id]}]
               ser-tx-info (au/<? (<make-ser-tx-info
                                   (u/sym-map actor-id client-id cmds root
                                              schema storage tx-id)))
               serializable-tx-infos [ser-tx-info]
               snapshot-interval 1
               arg (u/sym-map *branch->crdt-info actor-id authorizer branch
                              bulk-storage root schema serializable-tx-infos
                              snapshot-interval storage)
               lp-ret (au/<? (server/<log-producer-tx-batch! arg))
               _ (is (= true lp-ret))
               _ (is (= book (-> (@*branch->crdt-info branch)
                                 (:v)
                                 (:books)
                                 (get book-id))))
               _ (is (= {:actor-id-to-log-info {}
                         :branch-tx-ids [tx-id]}
                        (au/<? (storage/<get storage branch-log-k
                                             shared/branch-log-info-schema))))
               gcsi-ret (au/<? (server/<get-consumer-sync-info arg))
               {:keys [snapshot-url]} gcsi-ret
               snapshot (au/<? (common/<get-snapshot-from-url
                                (assoc arg :url snapshot-url)))
               _ (is (= book (-> snapshot :v :books (get book-id))))
               _ (is (= [book-id] (-> snapshot :crdt :children :books
                                      :children keys)))])
         (finally
           (bulk-storage/<stop-server! bulk-storage)))))))
