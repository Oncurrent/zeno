(ns oncurrent.zeno.crdt.common
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:array :map :record :union})

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti check-key (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmulti get-value (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmethod check-key :array
  [{:keys [add-id key op-type path]}]
  (when-not (string? key)
    (throw (ex-info (str "Array node-id key is not a string. Got: `"
                         (or key "nil") "`.")
                    (u/sym-map add-id key op-type path)))))

(defmethod check-key :map
  [{:keys [add-id key op-type path]}]
  (when-not (string? key)
    (throw (ex-info (str "Map key is not a string. Got: `"
                         (or key "nil") "`.")
                    (u/sym-map add-id key op-type path)))))

(defmethod check-key :record
  [{:keys [add-id key op-type path]}]
  (when-not (keyword? key)
    (throw (ex-info
            (str "Record key in path is not a keyword. "
                 "Got: `" (or key "nil") "`.")
            (u/sym-map key path op-type add-id)))))

(defn associative-get-value
  [{:keys [get-child-schema crdt path] :as arg}]
  (if (empty? path)
    (reduce-kv (fn [acc k _]
                 (let [v (get-value (assoc arg
                                           :path [k]))]
                   (if (or (nil? v)
                           (and (coll? v) (empty? v)))
                     acc
                     (assoc acc k v))))
               {}
               (:children crdt))
    (let [[k & ks] path]
      (check-key (assoc arg :key k))
      (get-value (assoc arg
                        :crdt (get-in crdt [:children k])
                        :schema (get-child-schema k)
                        :path ks)))))

;; TODO: Replace this with conflict resolution?
(defn get-most-recent [current-add-id-to-value-info]
  (->> (vals current-add-id-to-value-info)
       (sort-by :sys-time-ms)
       (last)))

(defn get-single-value [{:keys [crdt schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (when (pos? (count current-add-id-to-value-info))
      (-> (get-most-recent current-add-id-to-value-info)
          (:value)))))

(defmethod get-value :single-value
  [{:keys [path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT." arg))
    (get-single-value arg)))

(defmethod get-value :map
  [{:keys [schema] :as arg}]
  (let [get-child-schema #(l/schema-at-path schema [%])]
    (associative-get-value (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value :record
  [{:keys [schema] :as arg}]
  (let [get-child-schema #(l/schema-at-path schema [%])]
    (associative-get-value (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value :union
  [{:keys [crdt schema] :as arg}]
  (let [member-schema (l/member-schema-at-branch schema (:union-branch crdt))]
    (get-value (assoc arg :schema member-schema))))
