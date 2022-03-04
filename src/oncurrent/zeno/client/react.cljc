(ns oncurrent.zeno.client.react
  (:require
   #?(:cljs ["react" :as React])
   #?(:cljs ["react-dom" :as ReactDOM])
   #?(:cljs ["react-dom/server" :as ReactDOMServer])
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.client.impl :as client]
   [oncurrent.zeno.client.macro-impl :as macro-impl]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]
   #?(:cljs [goog.object])
   #?(:cljs [oops.core :refer [oapply ocall oget oset!]]))
  #?(:cljs
     (:require-macros oncurrent.zeno.client.react)))

(defn create-element [& args]
  #?(:cljs
     (oapply React :createElement args)))

(defn valid-element? [el]
  #?(:cljs
     (ocall React :isValidElement el)))

(defn render
  ([el container]
   (render el container (constantly nil)))
  ([el container cb]
   #?(:cljs
      (ocall ReactDOM :render el container cb))))

(defn render-to-string [el]
  #?(:cljs
     (ocall ReactDOMServer :renderToString el)))

(defn render-to-static-markup [el]
  #?(:cljs
     (ocall ReactDOMServer :renderToStaticMarkup el)))

(defn use-effect
  ([effect]
   #?(:cljs
      (ocall React :useEffect effect)))
  ([effect dependencies]
   #?(:cljs
      (ocall React :useEffect effect dependencies))))

(defn use-layout-effect
  ([effect]
   #?(:cljs
      (ocall React :useLayoutEffect effect)))
  ([effect dependencies]
   #?(:cljs
      (ocall React :useLayoutEffect effect dependencies))))

(defn use-reducer
  [reducer initial-state]
  #?(:cljs
     (ocall React :useReducer reducer initial-state)))

(defn use-ref
  ([]
   (use-ref nil))
  ([initial-value]
   #?(:cljs
      (ocall React :useRef initial-value))))

(defn use-state [initial-state]
  #?(:cljs
     (ocall React :useState initial-state)))

(defn use-callback [f deps]
  #?(:cljs
     (ocall React :useCallback f deps)))

(defn use-context [context]
  #?(:cljs
     (ocall React :useContext context)))

(defn create-context
  ([]
   #?(:cljs
      (React/createContext)))
  ([default-value]
   #?(:cljs
      (React/createContext default-value))))

(defn with-key [element k]
  "Adds the given React key to element."
  #?(:cljs
     (ocall React :cloneElement element #js {"key" k})))

;; We redirect to impl to avoid a cyclic ns dependency.
(defn batch-updates [&args]
  (apply impl/batch-updates args))

;;;; Macros

(defmacro def-component
  "Defines a Zeno React component.
   The first argument to the constructor must be a parameter named
  `zc` (the zeno client)."
  [component-name & args]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))]            ;; clj
    (macro-impl/build-component ns-name component-name args)))

(defmacro def-component-method
  "Defines a Zeno React component multimethod"
  [component-name dispatch-val & args]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))]            ;; clj
    (macro-impl/build-component ns-name component-name args dispatch-val)))


;;;; Custom Hooks

(defn use-zeno-state
  "React hook for Zeno state subscriptions.
   Note that the component-name parameter must be unique for each invocation."
  ([zc sub-map component-name]
   (use-zeno-state zc sub-map component-name {} []))
  ([zc sub-map component-name resolution-map]
   (use-zeno-state zc sub-map component-name resolution-map []))
  ([zc sub-map component-name resolution-map parents]
   #?(:cljs
      (let [[_ render!] (use-state nil)
            subscribe*! #(let [opts {:parents parents
                                     :react? true
                                     :resolution-map resolution-map}
                               update-fn (fn [new-state]
                                           (render! (u/current-time-ms)))]
                           (client/subscribe-to-state!
                             zc component-name sub-map update-fn opts))
            cleanup-effect (fn []
                             #(client/unsubscribe-from-state!
                                zc component-name))
            sub-info (client/get-subscription-info zc component-name)]
        (use-effect cleanup-effect #js [])
        (if (not sub-info)
          (subscribe*!)
          (if (= resolution-map (:resolution-map sub-info))
            (:state sub-info)
            (do
              (client/unsubscribe-from-state! zc component-name)
              (subscribe*!))))))))

(defn use-topic-subscription
  "React hook for Zeno topic subscriptions."
  ([zc topic-name cb]
   #?(:cljs
      (use-effect #(client/subscribe-to-topic! zc topic-name cb))))
  ([zc topic-name cb dependencies]
   #?(:cljs
      (use-effect #(client/subscribe-to-topic! zc topic-name cb)
                  dependencies))))

;;;;;;;;;;;;;;;;;;;; Macro runtime helpers ;;;;;;;;;;;;;;;;;;;;
;; Emitted code calls these fns

(defn get* [js-obj k]
  #?(:cljs
     (goog.object/get js-obj k)))

(defn js-obj* [kvs]
  #?(:cljs
     (apply goog.object/create kvs)))

(def ^:dynamic *parents* [])
