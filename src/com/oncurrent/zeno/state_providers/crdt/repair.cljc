(ns com.oncurrent.zeno.state-providers.crdt.repair
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.array :as array]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))


(defmulti repair (fn [{:keys [schema]}]
                   (c/schema->dispatch-type schema)))

(defn resolve-conflict-by-sys-time-ms
  [{:keys [crdt make-id] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt
        make-id* (or make-id u/compact-random-uuid)
        winner (->> current-add-id-to-value-info
                    (sort-by #(-> % second :sys-time-ms))
                    (last))
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
        crdt-ops (conj del-ops add-op)
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defmethod repair :array
  [{:keys [crdt] :as arg}]
  (let [info (array/get-array-info arg)]
    (cond
      (not (:linear? info))
      (array/repair-array arg)

      (empty? (:ordered-node-ids crdt))
      (-> arg
          (assoc-in [:crdt :ordered-node-ids] (:ordered-node-ids info))
          (assoc :crdt-ops #{}))

      :else
      (assoc arg :crdt-ops #{}))))

(defn xf-op-paths [{:keys [prefix crdt-ops]}]
  (reduce (fn [acc op]
            (conj acc (update op :op-path #(cons prefix %))))
          #{}
          crdt-ops))

(defn associative-repair
  [{:keys [crdt get-child-path-info get-child-schema path schema array?]
    :as arg}]
  (let [repair-child (fn [{:keys [k sub-path]}]
                       (let [ret (repair (assoc arg
                                                :crdt (get-in crdt [:children k])
                                                :path sub-path
                                                :schema (get-child-schema k)))]
                         (update ret :crdt-ops
                                 #(xf-op-paths {:prefix k
                                                :crdt-ops %}))))]
    (if (empty? path)
      (reduce-kv (fn [acc k _]
                   (let [ret (repair (assoc arg :schema (get-child-schema k)))]
                     (-> acc
                         (assoc :crdt (:crdt ret))
                         (update :crdt-ops set/union (:crdt-ops ret)))))
                 {:crdt crdt
                  :crdt-ops #{}}
                 (:children crdt))
      (-> path
          (get-child-path-info)
          (repair-child)))))

(defmethod repair :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (associative-repair
     (assoc arg
            :get-child-path-info (fn [[k & sub-path]]
                                   (u/sym-map k sub-path))
            :get-child-schema (constantly values-schema)))))

(defmethod repair :record
  [{:keys [path schema] :as arg}]
  (associative-repair
   (assoc arg
          :get-child-path-info (fn [[k & sub-path]]
                                 (u/sym-map k sub-path))
          :get-child-schema (fn [k]
                              (or (l/child-schema schema k)
                                  (throw (ex-info (str "Bad record key `" k
                                                       "` in path `"
                                                       path "`.")
                                                  (u/sym-map path k))))))))

(defmethod repair :union
  [{:keys [crdt path schema] :as arg}]
  (let [{:keys [union-branch]} crdt
        member-schema (when union-branch
                        (l/member-schema-at-branch schema union-branch))]
    (if-not member-schema
      {:crdt crdt
       :crdt-ops #{}}
      (let [ret (repair (assoc arg
                               :path path
                               :schema member-schema))]
        (update ret :crdt-ops
                #(xf-op-paths {:prefix union-branch
                               :crdt-ops %}))))))

(defmethod repair :single-value
  [{:keys [crdt resolve-conflict schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt
        num-vals (count current-add-id-to-value-info)]
    (case num-vals
      0 {:crdt crdt
         :crdt-ops #{}}
      1 {:crdt crdt
         :crdt-ops #{}}
      (let [resolve* (or resolve-conflict resolve-conflict-by-sys-time-ms)]
        (resolve* arg)))))
