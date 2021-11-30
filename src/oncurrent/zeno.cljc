(ns oncurrent.zeno
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.client :as client]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

#_(defn zeno-client [config]
    (client/zeno-client config))

(defn shutdown-client! [zc]
  (ca/close! (:cmd-ch zc)))

(defn log-in!
  ([zc branch-id authenticator-name authentication-info]
   (log-in! zc branch-id authenticator-name authentication-info nil))
  ([zc branch-id authenticator-name authentication-info cb]
   (let [cmd {:arg (u/sym-map branch-id authenticator-name authentication-info)
              :type :log-in
              :cb cb}]
     (ca/put! (:cmd-ch zc) cmd))))

(defn log-out!
  ([zc]
   (log-out! zc nil))
  ([zc cb]
   (let [cmd {:type :log-out
              :cb cb}]
     (ca/put! (:cmd-ch zc) cmd))))



;; (defn <update-state! [zc update-commands]
;;   (client/<update-state! zc update-commands))

;; (defn update-state!
;;   ([zc update-commands]
;;    (update-state! zc update-commands (constantly nil)))
;;   ([zc update-commands cb]
;;    (ca/go
;;      (try
;;        (cb (au/<? (client/<update-state! zc update-commands)))
;;        (catch #?(:clj Exception :cljs js/Error) e
;;          (cb e))))))

;; (defn <set-state! [zc path arg]
;;   (let [cmds [{:path path
;;                :op :set
;;                :arg arg}]]
;;     (<update-state! zc cmds)))

;; (defn set-state!
;;   ([zc path arg]
;;    (set-state! zc path arg (constantly nil)))
;;   ([zc path arg cb]
;;    (ca/go
;;      (try
;;        (cb (au/<? (<set-state! zc path arg)))
;;        (catch #?(:clj Exception :cljs js/Error) e
;;          (cb e))))))

;; (defn <get-state [zc path]
;;   (client/<get-state zc path))

;; (defn get-state [zc path cb]
;;   (ca/go
;;     (try
;;       (cb (au/<? (<get-state zc path)))
;;       (catch #?(:clj Exception :cljs js/Error) e
;;         (cb e)))))
