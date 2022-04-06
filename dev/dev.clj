(ns dev
  (:require [com.oncurrent.zeno.crdt :as crdt]
            [com.oncurrent.zeno.crdt.commands :as commands]
            [com.oncurrent.zeno.utils :as u]
            [deercreeklabs.lancaster :as l]
            [taoensso.timbre :as log]))

(let [sys-time-ms (u/str->long "1643061294782")
      value [[1 2] [3]]
      path [:zeno/crdt]
      arg {:cmds [{:zeno/arg value
                   :zeno/op :zeno/set
                   :zeno/path path}]
           :crdt-schema (l/array-schema (l/array-schema l/int-schema))
           :sys-time-ms sys-time-ms}]
  (commands/process-cmds arg)
  )
