(ns com.oncurrent.zeno.state-providers.crdt.apply-ops
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(def container-op-types #{:add-container :delete-container})
(def array-op-types #{:add-array-node :delete-array-node})

(defmulti apply-op
  (fn [{:keys [op-type schema]}]
    (let [sch-type (c/schema->dispatch-type schema)]
      (cond
        (= :union sch-type)
        [:union :any]

        (and (container-op-types op-type)
             (c/container-types sch-type))
        [:container op-type]

        (and (array-op-types op-type)
             (= :array sch-type))
        [:array op-type]

        (and (array-op-types op-type)
             (c/container-types sch-type))
        [:container :array-op]

        (array-op-types op-type)
        [:single-value :array-op]

        :else
        [sch-type op-type]))))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt sys-time-ms] :as arg}]
  (if (some-> (:deleted-add-ids crdt)
              (get add-id))
    crdt
    (let [value (if (contains? arg :value)
                  (:value arg)
                  (c/deserialize-op-value arg))
          value-info (u/sym-map sys-time-ms value)]
      (update crdt :add-id-to-value-info assoc add-id value-info))))

(defmethod apply-op [:single-value :delete-value]
  [{:keys [add-id crdt op-path] :as arg}]
  (if (some-> (:deleted-add-ids crdt)
              (get add-id))
    crdt
    (-> crdt
        (update :add-id-to-value-info dissoc add-id)
        (update :deleted-add-ids #(conj (or % #{}) add-id)))))

(defn apply-op-to-child
  [{:keys [add-id crdt growing-path op-path op-type schema shrinking-path]
    :as arg}]
  (let [[k & ks] shrinking-path
        new-growing-path (conj growing-path k)
        is-record? (= :record (l/schema-type schema))]
    (c/check-key (assoc arg
                        :key k
                        :path new-growing-path
                        :string-array-keys? true))
    (update-in crdt [:children k]
               (fn [child-crdt]
                 (apply-op (assoc arg
                                  :crdt child-crdt
                                  :growing-path new-growing-path
                                  :schema (if is-record?
                                            (l/child-schema schema k)
                                            (l/child-schema schema))
                                  :shrinking-path ks))))))

(defmethod apply-op [:container :add-container]
  [{:keys [add-id crdt shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (if (some-> (:deleted-container-add-ids crdt)
                (get add-id))
      crdt
      (update crdt :container-add-ids #(conj (or % #{}) add-id)))))

(defmethod apply-op [:container :delete-container]
  [{:keys [add-id crdt shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (if (some-> (:deleted-container-add-ids crdt)
                (get add-id))
      crdt
      (-> crdt
          (update :container-add-ids #(disj (or % #{}) add-id))
          (update :deleted-container-add-ids #(conj (or % #{}) add-id))))))

(defmethod apply-op [:single-value :add-container]
  [{:keys [crdt] :as arg}]
  ;; This can be called from apply-union-op
  crdt)

(defmethod apply-op [:single-value :delete-container]
  [{:keys [crdt] :as arg}]
  ;; This can be called from apply-union-op
  crdt)

(defmethod apply-op [:array :add-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child (assoc arg :string-array-keys? true))
    (throw (ex-info (str "Invalid op. Can't `:add-value` at array root. "
                         "`:op-path` should contain an array index.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:map :add-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:add-value` at map root. "
                         "`:op-path` should contain a map key.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:record :add-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:add-value` at record root. "
                         "`:op-path` should contain a record field key.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:union :any]
  [{:keys [add-id crdt growing-path op-path op-type schema shrinking-path
           sys-time-ms value]
    :as arg}]
  (let [[union-branch & ks] shrinking-path
        _ (when-not (int? union-branch)
            (throw (ex-info (str "Union op paths must incluce an explicit "
                                 "union branch index.")
                            (u/sym-map add-id growing-path op-path op-type
                                       shrinking-path value))))
        member-schema (nth (l/member-schemas schema) union-branch)
        branch-k (keyword (str "branch-" union-branch))
        branch-time-k (keyword (str "branch-"  union-branch "-sys-time-ms"))
        add-op? (shared/add-op-types op-type)]
    (cond-> crdt
      add-op? (assoc branch-time-k sys-time-ms)
      true (update branch-k
                   (fn [child-crdt]
                     (apply-op (assoc arg
                                      :crdt child-crdt
                                      :growing-path (conj growing-path
                                                          union-branch)
                                      :schema member-schema
                                      :shrinking-path ks)))))))

(defmethod apply-op [:array :delete-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child (assoc arg :string-array-keys? true))
    (throw (ex-info (str "Invalid op. Can't `:delete-value` at array root. "
                         "`:op-path` should contain an array index.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:map :delete-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:delete-value` at map root. "
                         "`:op-path` should contain a map key.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:record :delete-value]
  [{:keys [op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:delete-value` at record root. "
                         "`:op-path` should contain a record field key.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:single-value :array-op]
  [{:keys [op-path op-type schema shrinking-path] :as arg}]
  (let [schema-type (l/schema-type schema)]
    (throw (ex-info
            "Invalid op. Can't apply an array operation on a single value."
            (u/sym-map op-type op-path schema-type shrinking-path)))))

(defmethod apply-op [:container :array-op]
  [{:keys [op-path op-type schema shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child (assoc arg :string-array-keys? true))
    (let [schema-type (l/schema-type schema)]
      (throw (ex-info (str "Invalid op. Can't apply an array operation on "
                           "a " (name schema-type) ".")
                      (u/sym-map op-type op-path schema-type))))))

(defn causal-comparator [x y]
  (cond
    ((:peer-node-ids x) (:node-id y)) -1
    ((:peer-node-ids y) (:node-id x)) 1
    :else (compare (:node-id x) (:node-id y))))

(defn causal-traversal
  [{:keys [deleted-node-ids node-id-to-child-infos start-node-id] :as arg}]
  (loop [node-id start-node-id
         out (:out arg)]
    (let [infos (get node-id-to-child-infos node-id)]
      (case (count infos)
        0 out
        1 (let [new-node-id (-> infos first :node-id)
                _ (when (= node-id new-node-id)
                    (throw (ex-info "Node points to itself."
                                    (u/sym-map node-id
                                               node-id-to-child-infos
                                               infos))))
                new-out (if (get deleted-node-ids new-node-id)
                          out
                          (conj out new-node-id))]
            (recur new-node-id new-out))
        (reduce (fn [acc child-node-info]
                  (let [child-node-id (:node-id child-node-info)
                        new-out (if (get deleted-node-ids child-node-id)
                                  acc
                                  (conj acc child-node-id))]
                    (causal-traversal (assoc arg
                                             :out new-out
                                             :start-node-id child-node-id))))
                out
                (sort-by identity causal-comparator infos))))))

(defn calc-ordered-node-ids [crdt]
  (causal-traversal (assoc crdt
                           :out []
                           :start-node-id c/array-head-node-id)))

(defmethod apply-op [:array :add-array-node]
  [{:keys [crdt shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child (assoc arg :string-array-keys? true))
    (let [value (or (:value arg)
                    (c/deserialize-op-value
                     (assoc arg :schema shared/crdt-array-node-info-schema)))
          {:keys [parent-node-id]} value
          new-crdt (update-in crdt [:node-id-to-child-infos parent-node-id]
                              (fn [child-infos]
                                (conj child-infos value)))]
      (assoc new-crdt :ordered-node-ids (calc-ordered-node-ids new-crdt)))))

(defmethod apply-op [:array :delete-array-node]
  [{:keys [crdt node-id shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child (assoc arg :string-array-keys? true))
    (let [new-crdt (update crdt :deleted-node-ids assoc node-id)]
      (assoc new-crdt :ordered-node-ids (calc-ordered-node-ids new-crdt)))))

(defn apply-ops
  [{:keys [crdt crdt-ops data-schema root] :as arg}]
  (when-not (keyword? root)
    (throw (ex-info (str "Bad `:root` arg in call to `apply-ops`. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to `apply-ops`. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map data-schema))))
  (reduce (fn [acc {:keys [sys-time-ms op-path] :as op}]
            (apply-op (assoc op
                             :crdt acc
                             :growing-path [root]
                             :schema data-schema
                             :shrinking-path (rest op-path)
                             :sys-time-ms (or sys-time-ms
                                              (:sys-time-ms arg)
                                              (u/current-time-ms)))))
          crdt
          crdt-ops))
