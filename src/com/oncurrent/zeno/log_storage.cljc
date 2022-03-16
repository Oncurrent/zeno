(ns com.oncurrent.zeno.log-storage
  (:require
   [clojure.core.async :as ca]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [taoensso.timbre :as log]))

(def max-log-segment-size 1000)
(def tx-id->tx-info-key-prefix "_TX-ID-TO-TX-INFO-")

(defn <get-log-segment
  [{:keys [get-log-segment-k segment-id-k storage] :as arg}]
  (au/go
    (let [seg-id (au/<? (storage/<get
                         storage
                         segment-id-k
                         schemas/tx-log-id-schema))]
      (au/<? (storage/<get
              storage
              (get-log-segment-k {:segment-id seg-id})
              schemas/tx-log-segment-schema)))))

(defn <get-last-log-tx-i [arg]
  (au/go
    (if-let [tail-seg (au/<? (<get-log-segment
                              (assoc arg :segment-position :tail)))]
      (-> (:head-tx-i tail-seg)
          (+ (count (:tx-ids tail-seg)))
          (dec))
      -1)))

(defn <get-tx-info [{:keys [storage tx-id]}]
  (storage/<get storage
                (str tx-id->tx-info-key-prefix tx-id)
                schemas/tx-info-schema))

(defn <write-tx-info! [{:keys [storage tx-id tx-info]}]
  (storage/<swap! storage
                  (str tx-id->tx-info-key-prefix tx-id)
                  schemas/tx-info-schema
                  (constantly tx-info)))

(defn <write-new-segment
  [{:keys [get-log-segment-k
           head-segment-id-k
           head-tx-i
           previous-segment-id
           storage
           tail-segment-id-k
           tx-id]
    :as arg}]
  (au/go
    (let [new-seg-id (u/compact-random-uuid)
          new-seg {:head-tx-i head-tx-i
                   :tx-ids [tx-id]}
          new-seg-k (get-log-segment-k {:segment-id new-seg-id})
          prev-seg-k (when previous-segment-id
                       (get-log-segment-k {:segment-id previous-segment-id}))]
      (au/<? (storage/<swap! storage
                             new-seg-k
                             schemas/tx-log-segment-schema
                             (constantly new-seg)))
      (when prev-seg-k
        (au/<? (storage/<swap! storage
                               prev-seg-k
                               schemas/tx-log-segment-schema
                               #(assoc % :next-segment-id new-seg-id))))
      ;; TODO: Using `constantly` here is unsafe if concurrent writes happen
      (au/<? (storage/<swap! storage
                             tail-segment-id-k
                             schemas/tx-log-id-schema
                             (constantly new-seg-id)))
      (when-not prev-seg-k
        ;; TODO: Using `constantly` here is unsafe if concurrent writes happen
        (au/<? (storage/<swap! storage
                               head-segment-id-k
                               schemas/tx-log-id-schema
                               (constantly new-seg-id)))))))

(defn <write-tx-id-to-log!
  [{:keys [get-log-segment-k tail-segment-id-k storage tx-id] :as arg}]
  (au/go
    (let [tail-seg-id (au/<? (storage/<get
                              storage
                              tail-segment-id-k
                              schemas/tx-log-id-schema))
          tail-seg-k (when tail-seg-id
                       (get-log-segment-k {:segment-id tail-seg-id}))
          tail-seg (when tail-seg-k
                     (au/<? (storage/<get
                             storage
                             tail-seg-k
                             schemas/tx-log-segment-schema)))
          tail-seg-size (or (some-> tail-seg :tx-ids count) 0)]
      (cond
        ;; There is no log, so create one
        (nil? tail-seg-id)
        (au/<? (<write-new-segment (assoc arg :head-tx-i 0)))

        ;; Write a new segment after the tail
        (= tail-seg-size max-log-segment-size)
        (au/<? (<write-new-segment
                (assoc arg
                       :head-tx-i (+ (:head-tx-i tail-seg) tail-seg-size)
                       :previous-segment-id tail-seg-id)))

        :else ; Add on to the existing tail segment
        (au/<? (storage/<swap! storage
                               tail-seg-k
                               schemas/tx-log-segment-schema
                               (fn [old-seg]
                                 (-> old-seg
                                     (update :head-tx-i #(or % 0))
                                     (update :tx-ids conj tx-id)))))))))

(defn <get-log-range
  [{:keys [end-tx-i get-log-segment-k start-tx-i storage] :as arg}]
  (au/go
    (let [head-seg (au/<? (<get-log-segment
                           (assoc arg :segment-position :head)))
          <get-seg (fn [seg-id]
                     (storage/<get storage
                                   (get-log-segment-k {:segment-id seg-id})
                                   schemas/tx-log-segment-schema))]
      (if-not head-seg
        []
        (loop [seg head-seg
               out []]
          (let [{:keys [head-tx-i next-segment-id tx-ids]} seg
                last-tx-i (-> (count tx-ids)
                              (+ head-tx-i)
                              (dec))
                chunk-start (max head-tx-i start-tx-i)
                chunk-end (min last-tx-i end-tx-i)
                chunk (subvec tx-ids chunk-start (inc chunk-end))
                done? (<= end-tx-i last-tx-i)
                new-out (concat out chunk)]
            (if done?
              new-out
              (recur (au/<? (<get-seg next-segment-id))
                     new-out))))))))
