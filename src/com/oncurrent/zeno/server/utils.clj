(ns com.oncurrent.zeno.server.utils
  (:require
   [com.deercreeklabs.talk2.server :as t2s]
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  (:import
   (java.security SecureRandom)))

(defn make-schema-requester [{:keys [conn-id server]}]
  (fn [fp]
    (let [arg {:arg fp
               :conn-id conn-id
               :msg-type-name :get-schema-pcf-for-fingerprint
               :server server}]
      (t2s/<send-msg! arg))))

(defn secure-random-bytes
  "Returns a random byte array of the specified size or 32 by default."
  ([] (secure-random-bytes 32))
  ([size]
   (let [seed (byte-array size)]
     (.nextBytes (SecureRandom.) seed)
     seed)))
