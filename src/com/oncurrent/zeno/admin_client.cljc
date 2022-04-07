(ns com.oncurrent.zeno.admin-client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.impl :as impl]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn admin-client
  "Returns a Zeno admin client.
   Required config keys:
    - `:admin-password` - The admin password of the server
    - `:get-server-url` - A fn that returns the server url as a string
    "
  [config]
  (impl/zeno-client config))

(defn stop!
  "Stop the admin client and its connection to the server.
   Mostly useful in tests."
  [zc]
  (impl/stop! zc))

(defn <create-env!
  [{:keys [admin-client
           authenticators
           env-name
           root->state-provider]}]
  )

(defn <create-temporary-env!
  [{:keys [admin-client
           env-name
           lifetime-mins
           source-env-name]}]
  )

(defn <delete-env! [{:keys [admin-client]}]
  true)

(defn <get-env-names [{:keys [admin-client]}]
  [])
