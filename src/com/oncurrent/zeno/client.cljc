(ns com.oncurrent.zeno.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.impl :as impl]
   [com.oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn zeno-client
  "Returns a Zeno client."
  ([]
   (zeno-client {}))
  ([config]
   (impl/zeno-client config)))

(defn admin-client
  "Returns a Zeno admin client.
   Required config keys:
    - `:admin-password` - The admin password of the server
    - `:get-server-url` - A fn that returns the server url as a string
    "
  [config]
  (impl/zeno-client config))

(defn stop!
  "Stop the client and its connection to the server.
   Works with admin-clients as well as zeno-clients.
   Mostly useful in tests."
  [zc]
  (impl/stop! zc))

(defn update-state!
  ([zc update-cmds]
   (update-state! zc update-cmds (constantly nil)))
  ([zc update-cmds cb]
   (impl/update-state! zc update-cmds cb)))

(defn <update-state! [zc update-commands]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (impl/update-state! zc update-commands cb)
    ch))

(defn set-state!
  ([zc path arg]
   (set-state! zc path arg (constantly nil)))
  ([zc path arg cb]
   (impl/update-state! zc [{:zeno/path path
                            :zeno/op :zeno/set
                            :zeno/arg arg}] cb)))

(defn <set-state!
  ([zc path arg]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (set-state! zc path arg cb)
     ch)))

(defn get-subscription-info [zc state-sub-name]
  (when-let [info (-> zc :*state-sub-name->info deref (get state-sub-name))]
    (select-keys info [:state :resolution-map])))

(defn subscribe-to-state!
  "Creates a Zeno subscription. When the state or events referred to by any
   of the paths in the `sub-map` changes, `update-fn` is called.
   Note that this is a low-level function that generally
   should not be called directly. Prefer `react/def-component`
   or `react/use-zeno-state`.

   `opts` is a map of optional parameters:
     - `parents`: sequence of sub-names
     - `react?`: boolean. Enables react update batching
     - `resolution-map`: map of symbols to values to be used in resolving
                         symbols in values of the sub-map"
  ([zc state-sub-name sub-map update-fn]
   (subscribe-to-state! zc state-sub-name sub-map update-fn {}))
  ([zc state-sub-name sub-map update-fn opts]
   (state-subscriptions/subscribe-to-state! zc state-sub-name sub-map update-fn
                                            opts)))

(defn unsubscribe-from-state!
  "Cancels a state subscription and releases the related resources."
  [zc state-sub-name]
  (swap! (:*state-sub-name->info zc) dissoc state-sub-name)
  nil)

;; TODO: Unify the sub/unsub model for state & topic subscriptions
;; State has explicit unsub fn you call; topic sub returns an unsub fn.

(defn subscribe-to-topic!
  [zc topic-name cb]
  (impl/subscribe-to-topic! zc topic-name cb))

(defn publish-to-topic!
  [zc topic-name msg]
  (impl/publish-to-topic! zc topic-name msg))

(defn logged-in?
  [zc]
  (impl/logged-in? zc))

(defn rpc!
  "Calls a remote procedure on the server. If provided, calls callback
  `cb` with result."
  ([zc rpc-name-kw arg]
   (impl/rpc! (u/sym-map zc rpc-name-kw arg)))
  ([zc rpc-name-kw arg timeout-ms]
   (impl/rpc! (u/sym-map zc rpc-name-kw arg timeout-ms)))
  ([zc rpc-name-kw arg timeout-ms cb]
   (impl/rpc! (u/sym-map zc rpc-name-kw arg timeout-ms cb))))

(defn <rpc!
  "Calls a remote procedure on the server. Returns a channel which will yield
   the result."
  ([zc rpc-name-kw arg]
   (impl/<rpc! (u/sym-map zc rpc-name-kw arg)))
  ([zc rpc-name-kw arg timeout-ms]
   (impl/<rpc! (u/sym-map zc rpc-name-kw arg timeout-ms))))
