(ns oncurrent.zeno.client
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.client.impl :as impl]
   [oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [oncurrent.zeno.utils :as u]))

(defn zeno-client
  "Returns a Zeno client."
  ([]
   (zeno-client {}))
  ([config]
   (impl/zeno-client config)))

(defn shutdown!
  "Shutdown the zeno client and its connection to the server.
   Mostly useful in tests."
  [zc]
  (impl/shutdown! zc))

(defn update-state! [zc cmds cb]
  (impl/update-state! zc cmds cb))

(defn <update-state! [zc update-commands]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (impl/update-state! zc update-commands cb)
    ch))

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
