(ns com.oncurrent.zeno.state-providers.crdt.repair
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as aoi]
   [com.oncurrent.zeno.state-providers.crdt.array :as array]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn associative-repair
  [{:keys [crdt get-child-schema] :as arg}]
  (let [{:keys [children container-add-ids]} crdt]
    (if (empty? container-add-ids)
      arg
      (reduce-kv
       (fn [acc k child-crdt]
         (let [child-schema (get-child-schema k)
               ret (c/repair (assoc arg
                                    :crdt child-crdt
                                    :schema child-schema))
               ops (c/xf-op-paths {:prefix k
                                   :crdt-ops (:repair-crdt-ops ret)})]
           (-> acc
               (assoc-in [:crdt :children k] (:crdt ret))
               (update :repair-crdt-ops set/union ops))))
       {:crdt crdt
        :repair-crdt-ops #{}}
       children))))

(defmethod c/repair :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (associative-repair (assoc arg
                               :get-child-schema (constantly values-schema)))))

(defmethod c/repair :record
  [{:keys [path schema] :as arg}]
  (associative-repair (assoc arg :get-child-schema
                             (fn [k]
                               (or (l/child-schema schema k)
                                   (throw (ex-info (str "Bad record key `" k
                                                        "` in path `"
                                                        path "`.")
                                                   (u/sym-map path k))))))))

(defmethod c/repair :union
  [{:keys [crdt schema] :as arg}]
  (let [{:keys [union-branch]} crdt]
    (cond
      (not crdt)
      {:crdt nil
       :repair-crdt-ops #{}}

      (not union-branch)
      (throw (ex-info (str "Schema indicates a union, but no "
                           "`:union-branch` value is present.")
                      {:crdt-ks (keys crdt)}))

      :else
      (let [member-schema (l/member-schema-at-branch schema union-branch)
            ret (c/repair (assoc arg :schema member-schema))]
        {:crdt (assoc (:crdt ret) :union-branch union-branch)
         :repair-crdt-ops (c/xf-op-paths
                           {:prefix union-branch
                            :crdt-ops (:repair-crdt-ops ret)})}))))

(defn resolve-conflict-by-sys-time-ms
  [{:keys [crdt make-id schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt
        make-id* (or make-id u/compact-random-uuid)
        winner (->> current-add-id-to-value-info
                    (sort-by #(-> % second :sys-time-ms u/num->long))
                    (last)
                    (second))
        del-ops (reduce-kv (fn [acc add-id value-info]
                             (conj acc {:add-id add-id
                                        :op-path '()
                                        :op-type :delete-value}))
                           #{}
                           current-add-id-to-value-info)
        add-op {:add-id (make-id*)
                :op-path '()
                :op-type :add-value
                :sys-time-ms (:sys-time-ms winner)
                :value (:value winner)}
        ops (conj del-ops add-op)
        crdt* (aoi/apply-ops-without-repair (assoc arg :crdt-ops ops))]
    {:crdt crdt*
     :repair-crdt-ops ops}))

(defmethod c/repair :single-value
  [{:keys [crdt resolve-conflict schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt
        num-vals (count current-add-id-to-value-info)]
    (if (< num-vals 2)
      arg
      (let [resolve* (or resolve-conflict resolve-conflict-by-sys-time-ms)]
        (resolve* arg)))))
