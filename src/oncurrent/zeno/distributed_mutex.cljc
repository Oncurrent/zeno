(ns oncurrent.zeno.distributed-mutex
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def min-name-len 3)
(def max-name-len 200)
(def allowed-name-pattern (re-pattern
                           (str "[A-Za-z0-9_\\-\\.]"
                                "{" min-name-len "," max-name-len "}")))

(defn handle-acquisition! [mutex-client mutex-info]
  (let [{:keys [on-acquire *shutdown? *acquired?]} mutex-client]
    (when (and (not @*shutdown?)
               (not @*acquired?))
      (reset! *acquired? true)
      (on-acquire mutex-info))))

(defn handle-release! [mutex-client mutex-info]
  (let [{:keys [on-release *acquired?]} mutex-client]
    (when @*acquired?
      (reset! *acquired? false)
      (on-release mutex-info))))

(defn <refresh-mutex! [mutex-client old-mutex-info]
  (au/go
    (let [{:keys [client-name
                  lease-length-ms
                  mutex-key
                  *shutdown?
                  storage]} mutex-client
          new-mutex-info {:lease-id (u/compact-random-uuid)
                          :lease-length-ms lease-length-ms
                          :owner client-name}
          update-fn (fn [cur-mutex-info]
                      (if (= cur-mutex-info old-mutex-info)
                        new-mutex-info
                        (throw (ex-info "Not owner of the mutex!"
                                        (u/sym-map cur-mutex-info
                                                   new-mutex-info
                                                   old-mutex-info)))))]
      (when-not @*shutdown?
        (try
          (au/<? (storage/<swap! storage
                                 mutex-key
                                 schemas/mutex-info-schema
                                 update-fn))
          (handle-acquisition! mutex-client new-mutex-info)
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error (str "Lost mutex" (u/ex-msg-and-stacktrace e)))
            (handle-release! mutex-client old-mutex-info)))))))

(defn <attempt-acquisition! [mutex-client prior-lease-id]
  (au/go
    (let [{:keys [mutex-key
                  *shutdown?
                  storage]} mutex-client]
      (when (not @*shutdown?)
        (let [mutex-info (au/<? (storage/<get storage
                                              mutex-key
                                              schemas/mutex-info-schema))]
          (when (= prior-lease-id (:lease-id mutex-info))
            (au/<? (<refresh-mutex! mutex-client mutex-info))))))))

(defn start-watch-loop [mutex-client]
  (ca/go
    (try
      (let [{:keys [mutex-key
                    on-stale
                    *shutdown?
                    storage]} mutex-client]
        (loop [prior-lease-id nil]
          (let [mutex-info (au/<? (storage/<get storage
                                                mutex-key
                                                schemas/mutex-info-schema))
                {:keys [lease-length-ms lease-id]} mutex-info
                stale? (= prior-lease-id lease-id)]
            (when stale?
              (on-stale mutex-info)
              (reset! *shutdown? true))
            (ca/<! (ca/timeout lease-length-ms))
            (when-not @*shutdown?
              (recur lease-id)))))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error in watch loop:\n" (u/ex-msg-and-stacktrace e))))))

(defn start-aquire-loop [mutex-client]
  (ca/go
    (try
      (let [{:keys [client-name
                    mutex-key
                    on-stale
                    refresh-ratio
                    *shutdown?
                    storage]} mutex-client]
        (while (not @*shutdown?)
          (try
            (let [mutex-info (au/<? (storage/<get storage
                                                  mutex-key
                                                  schemas/mutex-info-schema))
                  {:keys [owner lease-length-ms lease-id]} mutex-info]
              (if (= client-name owner)
                (do
                  (handle-acquisition! mutex-client mutex-info)
                  (when (and lease-length-ms refresh-ratio)
                    (ca/<! (ca/timeout (/ lease-length-ms refresh-ratio))))
                  (au/<? (<refresh-mutex! mutex-client mutex-info)))
                (do
                  (handle-release! mutex-client mutex-info)
                  (when lease-length-ms
                    (ca/<! (ca/timeout lease-length-ms)))
                  (au/<? (<attempt-acquisition! mutex-client lease-id)))))
            (catch #?(:clj Exception :cljs js/Error) e
              (log/error (str "Error in aquire loop:\n"
                              (u/ex-msg-and-stacktrace e)))
              ;; Don't spin too fast if there is a repeating exception
              (ca/<! (ca/timeout 30000))))))
      (catch #?(:clj Exception :cljs js/Error) e
        (log/error "Error starting aquire loop:\n"
                   (u/ex-msg-and-stacktrace e))))))

(def default-mutex-client-options
  {:lease-length-ms 4000
   :on-acquire (constantly nil)
   :on-release (constantly nil)
   :on-stale (constantly nil)
   :refresh-ratio 3
   :retry-on-release? true
   :watch-only? false})

;;;;;;;;;;;;;;;;;;;;;;;;; External API ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn stop! [mutex-client]
  (reset! (:*shutdown? mutex-client) true)
  (handle-release! mutex-client nil))

(defn make-distributed-mutex-client
  ;; Options: (see `default-mutex-client-options` above for defaults.
  ;; - `:client-name` - Defaults to a compact-random-uuid (different for
  ;;                    each client)
  ;; - `:lease-length-ms` - After this amount of time (in milliseconds),
  ;;                        the lease is considered expired and other clients
  ;;                        can try to acquire the mutex.
  ;; - `:on-acquire` - Callback to be called when the mutex is
  ;;                   acquired by this client. Passed the mutex-info as arg.
  ;; - `:on-release` - Callback to be called when the mutex is
  ;;                   released by this client. Passed the mutex-info as arg.
  ;; - `:on-stale` - Callback to be called when the mutex becomes
  ;;                 stale. Passed the mutex-info as arg. Only called
  ;;                 if `:watch-only` is true.
  ;; - `:refresh-ratio` - Number of times during the lease period that the
  ;;                      client should refresh the mutex-info to show
  ;;                      liveness.
  ;; - `:watch-only?` - If true, watches the mutex, but never tries to acquire
  ;;                    it. If the mutex becomes stale, calls `:on-stale` and
  ;;                    stops watching the mutex.
  ;;
  ;; Retry behavior:
  ;; If the client acquires the mutex and then loses it for some reason
  ;; (failure to update it in a timely manner, network failure, etc.),
  ;; the client will keep trying to reaquire the mutex.
  ;; *Does not apply if the client was explicitly stopped (using the
  ;;  `stop!` fn).
  ([mutex-name storage]
   (make-distributed-mutex-client mutex-name storage {}))
  ([mutex-name storage options]
   (when-not (re-matches allowed-name-pattern mutex-name)
     (throw (ex-info
             (str "`mutex-name` is invalid. It must be " min-name-len "-"
                  max-name-len " characters long and contain only "
                  "letters, numbers, underscores, hyphens, or periods.")
             (u/sym-map mutex-name))))
   (let [{:keys [client-name
                 lease-length-ms
                 on-acquire
                 on-release
                 on-stale
                 retry-on-release
                 refresh-ratio
                 watch-only?]} (merge default-mutex-client-options
                                      {:client-name (u/compact-random-uuid)}
                                      options)
         mutex-key (str storage/mutex-reference-key-prefix mutex-name)
         *acquired? (atom false)
         *shutdown? (atom false)
         client (u/sym-map mutex-name mutex-key storage client-name
                           lease-length-ms on-acquire on-release on-stale
                           refresh-ratio retry-on-release watch-only?
                           *acquired? *shutdown?)]
     (if watch-only?
       (start-watch-loop client)
       (start-aquire-loop client))
     client)))
