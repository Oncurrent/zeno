;; This ns exists to avoid a cyclic ns dependency when
;; com.oncurrent.zeno.client.state-subscriptions requires
;; com.oncurrent.zeno.react which requires com.oncurrent.zeno.client which
;; requires com.oncurrent.zeno.client.state-subscriptions. Now
;; com.oncurrent.zeno.client.state-subscriptions can require this ns instead of
;; com.oncurrent.zeno.react and break that cycle.
(ns com.oncurrent.zeno.client.react.impl
  #?(:cljs
     (:require
       ["react-dom" :as ReactDOM]
       [oops.core :refer [ocall]])))

(defn batch-updates [f]
  #?(:cljs
     (ocall ReactDOM :unstable_batchedUpdates f)))
