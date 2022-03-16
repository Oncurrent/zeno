(ns com.oncurrent.zeno.server.storage
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def head-branch-log-segment-id-prefix "_HEAD-BRANCH-LOG-SEGMENT-ID-")
(def head-consumer-log-segment-id-prefix "_HEAD-CONSUMER-LOG-SEGMENT-ID-")
(def head-producer-log-segment-id-prefix "_HEAD-PRODUCER-LOG-SEGMENT-ID-")
(def log-segment-prefix "_LOG-SEGMENT-")
(def tail-branch-log-segment-id-prefix "_TAIL-BRANCH-LOG-SEGMENT-ID-")
(def tail-consumer-log-segment-id-prefix "_TAIL-CONSUMER-LOG-SEGMENT-ID-")
(def tail-producer-log-segment-id-prefix "_TAIL-PRODUCER-LOG-SEGMENT-ID-")

(defn get-log-segment-id-k
  [{:keys [actor-id branch client-id log-type segment-id segment-position]}]
  (str (case [log-type segment-position]
         [:branch :head] head-branch-log-segment-id-prefix
         [:branch :tail] tail-branch-log-segment-id-prefix
         [:consumer :head] head-consumer-log-segment-id-prefix
         [:consumer :tail] tail-consumer-log-segment-id-prefix
         [:producer :head] head-producer-log-segment-id-prefix
         [:producer :tail] tail-producer-log-segment-id-prefix
         (throw (ex-info (str "Unknown log type / segment position tuple: `"
                              [log-type segment-position] "`.")
                         (u/sym-map log-type segment-position))))
       branch "-"
       (when (= :producer log-type)
         (str client-id "-"))
       (when-not (= :branch log-type)
         (str actor-id "-"))
       segment-id))

(defn get-log-segment-k [{:keys [segment-id]}]
  (str log-segment-prefix segment-id))
