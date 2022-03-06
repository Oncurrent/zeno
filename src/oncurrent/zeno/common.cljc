(ns oncurrent.zeno.common
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def valid-ops (-> schemas/command-op-schema l/edn :symbols set))

(defn check-cmd [{:zeno/keys [arg op path] :as cmd}]
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
    (when-not (u/valid-path-roots head)
      (let [[head & tail] path
            disp-head (or head "nil")]
        (throw
         (ex-info (str "Paths must begin with one of " u/valid-path-roots
                       ". Got path `" path "` in command.")
                  (u/sym-map path head cmd)))))))

(defn <get-schema-from-peer [{:keys [<request-schema fp storage]}]
  (au/go
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
