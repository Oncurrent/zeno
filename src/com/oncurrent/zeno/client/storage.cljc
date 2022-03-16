(ns com.oncurrent.zeno.client.storage
  (:require
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def client-id-key "CLIENT-ID")
(def head-consumer-log-segment-id-prefix "_HEAD-CONSUMER-LOG-SEGMENT-ID-")
(def head-producer-log-segment-id-prefix "_HEAD-PRODUCER-LOG-SEGMENT-ID-")
(def tail-consumer-log-segment-id-prefix "_TAIL-CONSUMER-LOG-SEGMENT-ID-")
(def tail-producer-log-segment-id-prefix "_TAIL-PRODUCER-LOG-SEGMENT-ID-")
(def log-segment-prefix "_LOG-SEGMENT-")
(def login-session-token-key "_LOGIN-SESSION-TOKEN")

(defn get-log-segment-id-k
  [{:keys [*actor-id crdt-branch log-type segment-position]}]
  (str (case [log-type segment-position]
         [:consumer :head] head-consumer-log-segment-id-prefix
         [:consumer :tail] tail-consumer-log-segment-id-prefix
         [:producer :head] head-producer-log-segment-id-prefix
         [:producer :tail] tail-producer-log-segment-id-prefix
         (throw (ex-info (str "Unknown log type / segment position tuple: `"
                              [log-type segment-position] "`.")
                         (u/sym-map log-type segment-position))))
       crdt-branch "-" @*actor-id))

(defn get-log-segment-k [{:keys [segment-id]}]
  (str log-segment-prefix segment-id))
