(ns oncurrent.zeno.commands
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
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
