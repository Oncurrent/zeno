(ns com.oncurrent.zeno.state-providers.crdt.apply-ops
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti apply-op (fn [{:keys [op-type schema]}]
                     [(c/schema->dispatch-type schema) op-type]))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt op-path sys-time-ms] :as arg}]
  (if (some-> (:deleted-add-ids crdt)
              (get add-id))
    crdt
    (let [value (or (:value arg)
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
  [{:keys [crdt growing-path op-path schema shrinking-path] :as arg}]
  (let [[k & ks] shrinking-path
        new-growing-path (conj growing-path k)]
    (c/check-key (assoc arg :key k :path new-growing-path))
    (update-in crdt [:children k]
               (fn [child-crdt]
                 (apply-op (assoc arg
                                  :crdt child-crdt
                                  :growing-path new-growing-path
                                  :schema (l/child-schema schema)
                                  :shrinking-path ks))))))

(defmethod apply-op [:map :add-container]
  [{:keys [add-id crdt shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (if (some-> (:deleted-container-add-ids crdt)
                (get add-id))
      crdt
      (update crdt :container-add-ids #(conj (or % #{}) add-id)))))

(defmethod apply-op [:map :delete-container]
  [{:keys [add-id crdt growing-path schema shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (if (some-> (:deleted-container-add-ids crdt)
                (get add-id))
      crdt
      (-> crdt
          (update :container-add-ids #(disj (or % #{}) add-id))
          (update :deleted-container-add-ids #(conj (or % #{}) add-id))))))

(defmethod apply-op [:map :add-value]
  [{:keys [add-id crdt op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:add-value` at map root. "
                         "`:op-path` should contain a map key.")
                    (u/sym-map op-type op-path)))))

(defmethod apply-op [:map :delete-value]
  [{:keys [add-id crdt op-path op-type shrinking-path] :as arg}]
  (if (seq shrinking-path)
    (apply-op-to-child arg)
    (throw (ex-info (str "Invalid op. Can't `:delete-value` at map root. "
                         "`:op-path` should contain a map key.")
                    (u/sym-map op-type op-path)))))

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
