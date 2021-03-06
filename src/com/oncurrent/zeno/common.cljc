(ns com.oncurrent.zeno.common
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def valid-ops (-> schemas/command-op-schema l/edn :symbols set))

(defn check-cmd [{:keys [cmd valid-path-roots]}]
  (let [{:zeno/keys [arg op path]} cmd]
    (when-not (valid-ops op)
      (throw
       (ex-info (str "Invalid `:zeno/op` in command. Got `" op
                     "`. Valid `:zeno/op`s are: " valid-ops ".")
                (u/sym-map op cmd valid-ops))))
    (when-not (sequential? path)
      (throw (ex-info
              (str "The `:zeno/path` parameter of the update "
                   "command must be a sequence. Got: `"
                   path "`.")
              (u/sym-map cmd path))))
    (let [[head tail] path]
      (when-not (valid-path-roots head)
        (let [[head & tail] path
              disp-head (or head "nil")]
          (throw
           (ex-info (str "Paths must begin with one of " valid-path-roots
                         ". Got path `" path "` in command.")
                    (u/sym-map path head cmd valid-path-roots))))))))

(defn <get-schema-from-peer [{:keys [<request-schema fp storage]}]
  (au/go
    (when-not <request-schema
      (throw (ex-info (str "Can't request schema for `" fp `
                           "because no `:<request-schema` fn was provided.")
                      {:fp fp})))
    (let [json (au/<? (<request-schema fp))
          schema (l/json->schema json)
          ;; Call <schema->fp to store the schema in storage
          retrieved-fp (au/<? (storage/<schema->fp storage schema))]
      (when-not (ba/equivalent-byte-arrays? fp retrieved-fp)
        (let [fp-str (ba/byte-array->b64 fp)]
          (throw (ex-info (str "Failed to retrieve proper schema for fp `"
                               fp-str "` from peer.")
                          (u/sym-map fp fp-str retrieved-fp)))))
      schema)))

(defn <serialized-value->value
  [{:keys [reader-schema serialized-value storage] :as arg}]
  (au/go
    (when serialized-value
      (let [{:keys [fp bytes]} serialized-value
            writer-schema (or (au/<? (storage/<fp->schema storage fp))
                              (au/<? (<get-schema-from-peer
                                      (assoc arg :fp fp))))]
        (l/deserialize reader-schema writer-schema bytes)))))
