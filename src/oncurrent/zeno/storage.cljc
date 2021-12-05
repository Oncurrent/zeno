(ns oncurrent.zeno.storage
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IStorage
  (<add! [this value-k schema value])
  (<delete! [this k])
  (<fp->schema [this fp])
  (<get [this k schema])
  (<schema->fp [this schema])
  (<swap! [this reference-k schema update-fn]))

(defprotocol IRawStorage
  (<compare-and-set-k! [this k old-ba new-ba])
  (<delete-k! [this k])
  (<read-k [this k])
  (<add-k! [this k ba]))

;;;;;;;;;;;;; Storage Referenece Key Prefixes ;;;;;;;;

(def mutex-reference-key-prefix "_MUTEX_")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def max-swap-attempts 20)

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
             (<add-k! raw-storage k)
             (au/<?)))
      fp)))

(defn <get* [storage raw-storage k schema]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<get` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (let [ba (au/<? (<read-k raw-storage k))
          ser-val (when ba
                    (l/deserialize-same schemas/serialized-value-schema ba))
          {:keys [bytes fp]} ser-val
          writer-schema (when fp
                          (au/<? (<fp->schema storage fp)))]
      (when (and bytes writer-schema)
        (l/deserialize schema writer-schema bytes)))))

(defn <add!* [storage raw-storage value-k schema value]
  (au/go
    (when-not (string? value-k)
      (throw (ex-info (str "`value-k` argument to `<add!` must be a string. "
                           " Got `" value-k "`")
                      (u/sym-map value-k))))
    (when (str/starts-with? value-k "_")
      (throw (ex-info (str "`value-k` argument to `<add!` must not begin "
                           "with an underscore (`_`). Got `" value-k "`")
                      (u/sym-map value-k))))

    (let [bytes (l/serialize schema value)
          fp (au/<? (<schema->fp storage schema))
          ser-val (u/sym-map bytes fp)
          ba (l/serialize schemas/serialized-value-schema ser-val)]
      (au/<? (<add-k! raw-storage value-k ba)))))

(defn <swap!* [storage raw-storage reference-k schema update-fn]
  (au/go
    (when-not (string? reference-k)
      (throw (ex-info (str "`reference-k` argument to `<swap!` must be a "
                           "string. Got `" reference-k "`")
                      (u/sym-map reference-k))))
    (when-not (str/starts-with? reference-k "_")
      (throw (ex-info (str "`reference-k` argument to `<swap!` must begin "
                           "with an underscore (`_`). Got `" reference-k "`")
                      (u/sym-map reference-k))))
    (loop [attempts-remaining (dec max-swap-attempts)]
      (let [cur-ba (au/<? (<read-k raw-storage reference-k))
            cur-ser-val (when cur-ba
                          (l/deserialize-same schemas/serialized-value-schema
                                              cur-ba))
            {:keys [bytes fp]} cur-ser-val
            writer-schema (when fp
                            (au/<? (<fp->schema storage fp)))
            cur-val (when (and bytes writer-schema)
                      (l/deserialize schema writer-schema bytes))
            new-val (update-fn cur-val)
            new-ser-val {:bytes (l/serialize schema new-val)
                         :fp (au/<? (<schema->fp storage schema))}
            new-ba (l/serialize schemas/serialized-value-schema new-ser-val)
            success? (au/<? (<compare-and-set-k! raw-storage reference-k
                                                 cur-ba new-ba))]
        (cond
          success? new-val
          (pos? attempts-remaining) (recur (dec attempts-remaining))
          :else (throw
                 (ex-info
                  (str "`<swap!` for key `" reference-k "` failed after "
                       max-swap-attempts " attempts.")
                  (u/sym-map reference-k max-swap-attempts))))))))

(defrecord Storage [raw-storage]
  IStorage
  (<add! [this value-k schema value]
    (<add!* this raw-storage value-k schema value))

  (<delete! [this k]
    (<delete-k! raw-storage k))

  (<fp->schema [this fp]
    (<fp->schema* raw-storage fp))

  (<get [this k schema]
    (<get* this raw-storage k schema))

  (<schema->fp [this schema]
    (<schema->fp* raw-storage schema))

  (<swap! [this reference-k schema update-fn]
    (<swap!* this raw-storage reference-k schema update-fn)))

(defrecord MemRawStorage [*data]
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

  (<read-k [this k]
    (au/go
      (@*data k)))

  (<add-k! [this k ba]
    (au/go
      (swap! *data (fn [m]
                     (when (get m k)
                       (throw (ex-info (str "Key `" k "` already exists.")
                                       (u/sym-map k))))
                     (assoc m k ba)))
      true)))

(defn make-mem-raw-storage []
  (->MemRawStorage (atom {})))

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

(defn <serialized-value->value [storage reader-schema s-val]
  (au/go
    (let [{:keys [fp bytes]} s-val
          writer-schema (au/<? (<fp->schema storage fp))]
      (l/deserialize reader-schema writer-schema bytes))))

(defn <command->serializable-command [storage sys-schema cmd]
  (au/go
    (let [{:keys [arg path]} cmd
          value-schema (l/schema-at-path sys-schema path)
          s-val (au/<? (<value->serialized-value storage value-schema arg))]
      (assoc cmd :arg s-val))))

(defn <serializable-command->command [storage sys-schema s-cmd]
  (au/go
    (let [{:keys [arg path]} s-cmd
          reader-schema (l/schema-at-path sys-schema path)
          val (au/<? (<serialized-value->value storage reader-schema arg))]
      (assoc s-cmd :arg val))))
