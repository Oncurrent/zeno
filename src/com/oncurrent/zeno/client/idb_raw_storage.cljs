(ns com.oncurrent.zeno.client.idb-raw-storage
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [oops.core :refer [ocall ocall+ oget oset!]]
   [taoensso.timbre :as log]))

(defn <read-k* [idb store-name k]
  (let [ch (ca/chan)
        tx (ocall idb :transaction store-name "readonly")
        store (ocall tx :objectStore store-name)
        req (ocall store :get k)]
    (oset! req :onerror
           (fn [event]
             (ca/put! ch (ex-info (str "Error getting from indexedDB: \n"
                                       (oget req :error))
                                  (u/sym-map event store-name tx k req)))))
    (oset! req :onsuccess (fn [event]
                            (let [result (some-> (oget req :result)
                                                 (oget "v"))]
                              (if result
                                (ca/put! ch result)
                                (ca/close! ch)))))
    ch))

(defn <write-k!* [idb store-name op k v]
  (let [ch (ca/chan)
        tx (ocall idb :transaction store-name "readwrite")
        store (ocall tx :objectStore store-name)
        req (ocall+ store op #js {"k" k "v" v})]
    (oset! req :onerror
           (fn [event]
             (ca/put! ch (ex-info (str "Error writing to indexedDB: \n"
                                       (oget req :error))
                                  (u/sym-map event store-name tx k v req)))))
    (oset! req :onsuccess
           (fn [e]
             (ca/put! ch (= k (oget req :result)))))
    ch))

(defn <delete-k!* [idb store-name k]
  (let [ch (ca/chan)
        tx (ocall idb :transaction store-name "readwrite")
        store (ocall tx :objectStore store-name)
        req (ocall store :delete k)]
    (oset! req :onerror
           (fn [event]
             (ca/put! ch (ex-info (str "Error deleting from indexedDB: \n"
                                       (oget req :error))
                                  (u/sym-map event store-name tx k req)))))
    (oset! req :onsuccess #(ca/put! ch true))
    ch))

(defn <compare-and-set-k!* [idb store-name k old-ba new-ba]
  (let [ch (ca/chan)
        tx (ocall idb :transaction store-name "readwrite")
        store (ocall tx :objectStore store-name)
        req (ocall store :get k)]
    (oset! req :onerror
           (fn [event]
             (ca/put! ch (ex-info (str "Error getting from indexedDB: \n"
                                       (oget req :error))
                                  (u/sym-map event store-name tx k req)))))
    (oset! req :onsuccess
           (fn [event]
             (let [result (some-> (oget req :result)
                                  (oget "v"))]
               (if-not (ba/equivalent-byte-arrays? result old-ba)
                 (ca/put! ch false)
                 (let [w-req (ocall store :put #js {"k" k "v" new-ba})]
                   (oset! w-req :onerror
                          (fn [event]
                            (ca/put!
                             ch (ex-info
                                 (str "Error in <compare-and-set-k! while "
                                      "writing to indexedDB: \n"
                                      (oget w-req :error))
                                 (u/sym-map event store-name tx k w-req
                                            req old-ba new-ba)))))
                   (oset! w-req :onsuccess
                          (fn [e]
                            (let [ret (oget w-req :result)]
                              (ca/put! ch (= k ret))))))))))
    ch))

(defrecord IDBRawStorage [store-name idb-promise-chan]
  storage/IRawStorage
  (<compare-and-set-k! [this k old-ba new-ba]
    (au/go
      (-> (au/<? idb-promise-chan)
          (<compare-and-set-k!* store-name k old-ba new-ba)
          (au/<?))))

  (<delete-k! [this k]
    (au/go
      (-> (au/<? idb-promise-chan)
          (<delete-k!* store-name k)
          (au/<?))))

  (get-max-value-bytes [this]
    40000000)

  (<read-k [this k]
    (au/go
      (-> (au/<? idb-promise-chan)
          (<read-k* store-name k)
          (au/<?))))

  (<put-k! [this k ba]
    (au/go
      (-> (au/<? idb-promise-chan)
          (<write-k!* store-name :put k ba)
          (au/<?))))

  (<add-k! [this k ba]
    (au/go
      (-> (au/<? idb-promise-chan)
          (<write-k!* store-name :add k ba)
          (au/<?)))))

(defn <get-idb [db-name store-name db-version]
  (when (u/node?)
    (js/require "fake-indexeddb/auto"))
  (let [ch (ca/chan)
        op-req (ocall js/indexedDB :open db-name db-version)]
    (oset! op-req :onupgradeneeded
           (fn [e]
             (let [idb (oget op-req :result)
                   stores (set (oget idb :objectStoreNames))]
               (when-not (stores store-name)
                 (ocall idb :createObjectStore
                        store-name #js {:keyPath "k"})))))
    (oset! op-req :onerror
           (fn [e]
             (ca/put! ch (ex-info (str "Error opening indexedDB: \n"
                                       (u/ex-msg-and-stacktrace e))
                                  {:err e}))))
    (oset! op-req :onsuccess
           (fn [e]
             (ca/put! ch (oget op-req :result))))
    ch))

(defn make-idb-raw-storage
  ([db-name store-name]
   (make-idb-raw-storage db-name store-name 1))
  ([db-name store-name db-version]
   (let [idb-promise-chan (ca/promise-chan)]
     (ca/go
       (try
         (->> (<get-idb db-name store-name db-version)
              (au/<?)
              (ca/>! idb-promise-chan))
         (catch js/Error e
           (ca/>! idb-promise-chan e))))
     (->IDBRawStorage store-name idb-promise-chan))))
