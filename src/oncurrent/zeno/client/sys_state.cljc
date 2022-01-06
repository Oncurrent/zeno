(ns oncurrent.zeno.client.sys-state
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.client.client-log :as client-log]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn <path->item-id [{:keys [path storage]}]
  (au/go
    (let [root-item-id (au/<? (storage/<get
                               storage
                               storage/client-data-store-root-item-id-key
                               l/string-schema))]
      )))

(defmulti <cmd->info #(-> % :cmd :op))

(defmethod <cmd->info :set
  [{:keys [cmd sys-time-ms] :as arg*}]
  (au/go
    (let [{:keys [path]} cmd
          item-id (au/<? (<path->item-id (assoc arg* :path path)))
          base-op (u/sym-map item-id sys-time-ms)
          crdt-ops []
          update-infos []]
      (u/sym-map crdt-ops update-infos))))

(defn <cmds->info [{:keys [cmds] :as arg}]
  (au/go
    (let [initial-acc {:crdt-ops []
                       :update-infos []}
          last-i (dec (count cmds))]
      (if (neg? last-i)
        initial-acc
        (loop [i 0
               acc initial-acc]
          (let [cmd (nth cmds i)
                ret (au/<? (<cmd->info (assoc arg :cmd cmd)))
                new-acc (-> acc
                            (update :crdt-ops concat (:crdt-ops ret))
                            (update :update-infos concat (:update-infos ret)))]
            (if (= last-i i)
              new-acc
              (recur (inc i) new-acc))))))))

(defn <do-sys-updates! [zc update-cmds]
  ;; Should return update-infos
  (au/go
    (let [sys-time-ms (u/current-time-ms)
          arg {:cmds update-cmds
               :storage (:data-storage zc)
               :sys-schema (:sys-schema zc)
               :sys-time-ms sys-time-ms}
          {:keys [crdt-ops update-infos]} (au/<? (<cmds->info arg))
          tx-info (u/sym-map crdt-ops sys-time-ms update-cmds)
          _ (au/<? (client-log/<log-tx! zc tx-info))
          ds (crdt/apply-ops {:ops crdt-ops
                              :data-store @(:*sys-data-store zc)})]
      update-infos)))
