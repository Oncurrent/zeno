(ns oncurrent.zeno.storage
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defprotocol IStorage
  (<compare-and-set! [this k schema old-value new-value])
  (<delete! [this k])
  (<fp->schema [this fp])
  (<get [this k schema])
  (<schema->fp [this schema])
  (<put! [this k schema value]))

(defprotocol IRawStorage
  (<compare-and-set-k! [this k old-ba new-ba])
  (<delete-k! [this k])
  (<read-k [this k])
  (<write-k! [this k ba]))

(defn make-fp->schema-k [fp]
  (str "_FP-TO-SCHEMA-" (ba/byte-array->hex-str fp)))

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
             (<write-k! raw-storage k)
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

(defn <put!* [storage raw-storage k schema value]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<put!` must be a string. Got `"
                           k "`")
                      (u/sym-map k))))
    (let [bytes (l/serialize schema value)
          fp (au/<? (<schema->fp storage schema))
          ser-val (u/sym-map bytes fp)
          ba (l/serialize schemas/serialized-value-schema ser-val)]
      (au/<? (<write-k! raw-storage k ba)))))

(defn <compare-and-set!* [storage k schema old-value new-value]
  (au/go
    (when-not (string? k)
      (throw (ex-info (str "`k` argument to `<compare-and-set!` must be a "
                           "string. Got `" k "`")
                      (u/sym-map k))))
    (let [fp (au/<? (<schema->fp storage schema))
          old-ser-val (when old-value
                        {:bytes (l/serialize schema old-value)
                         :fp fp})
          new-ser-val {:bytes (l/serialize schema new-value)
                       :fp fp}
          old-ba (when old-ser-val
                   (l/serialize schemas/serialized-value-schema old-ser-val))
          new-ba (l/serialize schemas/serialized-value-schema new-ser-val)]
      (au/<? (<compare-and-set-k! (:raw-storage storage) k old-ba new-ba)))))

(defrecord Storage [raw-storage]
  IStorage
  (<compare-and-set! [this k schema old-value new-value]
    (<compare-and-set!* this k schema old-value new-value))

  (<delete! [this k]
    (<delete-k! raw-storage k))

  (<fp->schema [this fp]
    (<fp->schema* raw-storage fp))

  (<get [this k schema]
    (<get* this raw-storage k schema))

  (<schema->fp [this schema]
    (<schema->fp* raw-storage schema))

  (<put! [this k schema value]
    (<put!* this raw-storage k schema value)))

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

  (<write-k! [this k ba]
    (au/go
      (swap! *data assoc k ba)
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
