(ns com.oncurrent.zeno.storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IStorage
  (<add! [this value-k schema value])
  (<delete! [this k])
  (<fp->schema [this fp])
  (<get [this k schema])
  (<schema->fp [this schema])
  (<swap! [this reference-k schema update-fn]))

(defprotocol IRawStorage
  (<add-k! [this k ba])
  (<compare-and-set-k! [this k old-ba new-ba])
  (<delete-k! [this k])
  (get-max-value-bytes [this])
  (<put-k! [this k ba])
  (<read-k [this k]))

;;;;;;;;;;;;; Storage Keys & Prefixes ;;;;;;;;;;;;;;;;;;;

(def cluster-membership-list-reference-key "_CLUSTER-MEMBERSHIP-LIST")
(def member-info-key-prefix "MEMBER-INFO-")
(def mutex-reference-key-prefix "_MUTEX-")
(def login-session-token-to-token-info-key-prefix
  "LOGIN-SESSION-TOKEN-TO-TOKEN-INFO-")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def max-swap-attempts 42)

(defn make-fp->schema-k [fp]
  (str "FP-TO-SCHEMA-" (ba/byte-array->hex-str fp)))

(defn <fp->schema* [raw-storage fp]
  (au/go
    (some->> fp
             (make-fp->schema-k)
             (<read-k raw-storage)
             (au/<?)
             (l/deserialize-same l/string-schema)
             (l/json->schema))))

(defn <schema->fp* [raw-storage schema]
  (au/go
    (let [fp (l/fingerprint256 schema)
          k (make-fp->schema-k fp)]
      (when-not (au/<? (<read-k raw-storage k))
        (->> (l/json schema)
             (l/serialize l/string-schema)
             (<put-k! raw-storage k)
             (au/<?)))
      fp)))

(defn <read-chunks [raw-storage chunk-ids]
  (au/go
    (let [chunks-ch (-> (partial <read-k raw-storage)
                        (map chunk-ids)
                        (ca/merge))
          num-chunks (count chunk-ids)]
      (loop [chunk-i->bytes (sorted-map)]
        (let [ba (au/<? chunks-ch)
              chunk (l/deserialize-same schemas/chunk-schema ba)
              {:keys [chunk-i bytes]} chunk
              new-chunk-i->bytes (assoc chunk-i->bytes chunk-i bytes)]
          (if (< (count new-chunk-i->bytes) num-chunks)
            (recur new-chunk-i->bytes)
            (ba/concat-byte-arrays (vals new-chunk-i->bytes))))))))

(defn <store-as-chunks! [raw-storage ba]
  (au/go
    (let [chunk-size (get-max-value-bytes raw-storage)
          chunks (ba/byte-array->fragments ba chunk-size)
          num-chunks (count chunks)
          chunk-ids (take num-chunks (repeatedly u/compact-random-uuid))
          <write-chunk! (fn [chunk-i bytes]
                          (let [chunk-id (nth chunk-ids chunk-i)]
                            (->> (u/sym-map bytes chunk-i)
                                 (l/serialize schemas/chunk-schema)
                                 (<add-k! raw-storage chunk-id))))
          writes-ch (-> (map-indexed <write-chunk! chunks)
                        (ca/merge))]
      (loop [num-written 0]
        (au/<? writes-ch)
        (let [new-num-written (inc num-written)]
          (if (< new-num-written num-chunks)
            (recur new-num-written)
            chunk-ids))))))

(defn <ba->value [storage schema ba]
  (au/go
    (let [ser-val (when ba
                    (l/deserialize-same schemas/serialized-value-schema ba))
          {:keys [chunk-ids fp bytes]} ser-val
          writer-schema (when fp
                          (au/<? (<fp->schema storage fp)))]
      (cond
        (or (not ser-val)
            (not writer-schema)
            (and (empty? bytes)
                 (empty? chunk-ids)))
        nil

        (seq bytes)
        (l/deserialize schema writer-schema bytes)

        (seq chunk-ids)
        (->> (<read-chunks (:raw-storage storage) chunk-ids)
             (au/<?)
             (l/deserialize schema writer-schema))))))

(defn <value->ba [storage schema value]
  (au/go
    (let [{:keys [raw-storage]} storage
          bytes* (l/serialize schema value)
          inline? (<= (count bytes*) (get-max-value-bytes raw-storage))
          bytes (when inline?
                  bytes*)
          fp (au/<? (<schema->fp storage schema))
          chunk-ids (when-not inline?
                      (au/<? (<store-as-chunks! raw-storage bytes*)))
          ser-val (u/sym-map chunk-ids fp bytes)]
      (l/serialize schemas/serialized-value-schema ser-val))))

(defn <get* [storage k schema]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<get` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (let [{:keys [raw-storage]} storage]
      (some->> (<read-k raw-storage k)
               (au/<?)
               (<ba->value storage schema)
               (au/<?)))))

(defn <add!* [storage value-k schema value]
  (au/go
    (when-not (string? value-k)
      (throw (ex-info (str "`value-k` argument to `<add!` must be a string. "
                           " Got `" value-k "`")
                      (u/sym-map value-k))))
    (when (str/starts-with? value-k "_")
      (throw (ex-info (str "`value-k` argument to `<add!` must not begin "
                           "with an underscore (`_`). Got `" value-k "`")
                      (u/sym-map value-k))))
    (let [{:keys [raw-storage]} storage]
      (->> (<value->ba storage schema value)
           (au/<?)
           (<add-k! raw-storage value-k)
           (au/<?)))))

(defn <swap!* [storage reference-k schema update-fn]
  (au/go
    (when-not (string? reference-k)
      (throw (ex-info (str "`reference-k` argument to `<swap!` must be a "
                           "string. Got `" reference-k "`")
                      (u/sym-map reference-k))))
    (when-not (str/starts-with? reference-k "_")
      (throw (ex-info (str "`reference-k` argument to `<swap!` must begin "
                           "with an underscore (`_`). Got `" reference-k "`")
                      (u/sym-map reference-k))))
    (loop [attempts 1]
      (let [{:keys [raw-storage]} storage
            cur-ba (au/<? (<read-k raw-storage reference-k))
            cur-val (au/<? (<ba->value storage schema cur-ba))
            new-val (update-fn cur-val)
            new-ba (au/<? (<value->ba storage schema new-val))
            success? (au/<? (<compare-and-set-k! raw-storage reference-k
                                                 cur-ba new-ba))]
        (cond
          success?
          new-val

          (< attempts max-swap-attempts)
          (do
            (ca/<! (ca/timeout (rand-int (* 10 attempts))))
            (recur (inc attempts)))

          :else
          (throw
           (ex-info
            (str "`<swap!` for key `" reference-k "` failed after "
                 max-swap-attempts " attempts.")
            (u/sym-map reference-k max-swap-attempts))))))))

(defn <delete!* [raw-storage k]
  (au/go
    (let [ba (au/<? (<read-k raw-storage k))
          ser-val (when ba
                    (l/deserialize-same schemas/serialized-value-schema ba))
          {:keys [chunk-ids]} ser-val
          num-chunks (count chunk-ids)]
      (if (zero? num-chunks)
        (<delete-k! raw-storage k)
        (let [deletes-ch (-> (map #(<delete-k! raw-storage %) chunk-ids))]
          (loop [num-deleted 0]
            (au/<? deletes-ch)
            (let [new-num-deleted (inc num-deleted)]
              (if (< new-num-deleted num-chunks)
                (recur new-num-deleted)
                true))))))))

(defrecord Storage [raw-storage]
  IStorage
  (<add! [this value-k schema value]
    (<add!* this value-k schema value))

  (<delete! [this k]
    (<delete!* raw-storage k))

  (<fp->schema [this fp]
    (<fp->schema* raw-storage fp))

  (<get [this k schema]
    (<get* this k schema))

  (<schema->fp [this schema]
    (<schema->fp* raw-storage schema))

  (<swap! [this reference-k schema update-fn]
    (<swap!* this reference-k schema update-fn)))

(defn prefix-k [prefix k]
  (let [first-char (when (str/starts-with? k "_")
                     "_")]
    (str first-char prefix "-" k)))

(defrecord PrefixedStorage [prefix storage]
  IStorage
  (<add! [this value-k schema value]
    (<add! storage (prefix-k prefix value-k) schema value))

  (<delete! [this k]
    (<delete! storage (prefix-k prefix k)))

  (<fp->schema [this fp]
    (<fp->schema storage fp))

  (<get [this k schema]
    (<get storage (prefix-k prefix k) schema))

  (<schema->fp [this schema]
    (<schema->fp storage schema))

  (<swap! [this reference-k schema update-fn]
    (<swap! storage (prefix-k prefix reference-k) schema update-fn)))

(defn make-prefixed-storage [prefix storage]
  (->PrefixedStorage prefix storage))

(defrecord MemRawStorage [*data max-value-bytes]
  IRawStorage
  (<compare-and-set-k! [this k old-ba new-ba]
    (au/go
      ;; Can't use clojure's compare-and-set! here b/c byte arrays
      ;; don't compare properly.
      (let [new-data (swap! *data
                            (fn [m]
                              (if (ba/equivalent-byte-arrays?
                                   old-ba
                                   (m k))
                                (assoc m k new-ba)
                                m)))]
        (ba/equivalent-byte-arrays? new-ba (new-data k)))))

  (<delete-k! [this k]
    (au/go
      (swap! *data dissoc k)
      true))

  (get-max-value-bytes [this]
    max-value-bytes)

  (<read-k [this k]
    (au/go
      (@*data k)))

  (<put-k! [this k ba]
    (au/go
      (swap! *data (fn [m]
                     (assoc m k ba)))
      true))

  (<add-k! [this k ba]
    (au/go
      (swap! *data (fn [m]
                     (when (get m k)
                       (throw (ex-info (str "Key `" k "` already exists.")
                                       {:type :key-exists
                                        :k k})))
                     (assoc m k ba)))
      true)))

(defn make-mem-raw-storage
  ([]
   (make-mem-raw-storage 5000000))
  ([max-value-bytes]
   (->MemRawStorage (atom {}) max-value-bytes)))

(defn make-storage
  ([]
   (make-storage (make-mem-raw-storage)))
  ([raw-storage]
   (->Storage raw-storage)))

(defn <value->serialized-value [storage schema value]
  (au/go
    (let [fp (au/<? (<schema->fp storage schema))
          bytes (l/serialize schema value)]
      (u/sym-map fp bytes))))

(defn <serialized-value->value [storage reader-schema serialized-value]
  (au/go
    (when serialized-value
      (let [{:keys [fp bytes]} serialized-value
            writer-schema (au/<? (<fp->schema storage fp))]
        (when-not writer-schema
          (let [fp-str (ba/byte-array->b64 fp)]
            (throw (ex-info (str "Failed to retrieve schema for fp `"
                                 fp-str "`.")
                            (u/sym-map fp fp-str)))))
        (l/deserialize reader-schema writer-schema bytes)))))
