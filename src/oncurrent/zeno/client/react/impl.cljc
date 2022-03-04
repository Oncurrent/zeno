;; This ns exists to avoid a cyclic ns dependency when
;; oncurrent.zeno.client.state-subscriptions requires
;; oncurrent.zeno.react which requires oncurrent.zeno.client which
;; requires oncurrent.zeno.client.state-subscriptions. Now
;; oncurrent.zeno.client.state-subscriptions can require this ns instead of
;; oncurrent.zeno.react and break that cycle.
(ns oncurrent.zeno.client.react.impl
  #?(:cljs
     (:require
       ["react-dom" :as ReactDOM]
       [oops.core :refer [ocall]])))

(defn batch-updates [f]
  #?(:cljs
     (ocall ReactDOM :unstable_batchedUpdates f)))
