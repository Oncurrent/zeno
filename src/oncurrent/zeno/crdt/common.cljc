(ns oncurrent.zeno.crdt.common
  (:require
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:array :map :record :union})

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti check-key (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmulti get-value-info (fn [{:keys [schema]}]
                           (schema->dispatch-type schema)))

(defmethod check-key :array
  [{:keys [add-id key op-type string-array-keys? path]}]
  (if string-array-keys?
    (when-not (string? key)
      (throw (ex-info (str "Array key must be a string. Got: `"
                           (or key "nil") "`.")
                      (u/sym-map add-id key op-type path))))
    (when-not (int? key)
      (throw (ex-info (str "Array index must be an integer. Got: `"
                           (or key "nil") "`.")
                      (u/sym-map add-id key op-type path))))))

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

(defn associative-get-value-info
  [{:keys [get-child-schema crdt norm-path path] :as arg}]
  (if (empty? path)
    (let [value (reduce-kv
                 (fn [acc k _]
                   (let [v (-> (assoc arg :path [k])
                               (get-value-info)
                               (:value))]
                     (if (or (nil? v)
                             (and (coll? v) (empty? v)))
                       acc
                       (assoc acc k v))))
                 {}
                 (:children crdt))]
      (u/sym-map value norm-path))
    (let [[k & ks] path]
      (if-not k
        {:norm-path norm-path
         :value nil}
        (do
          (check-key (assoc arg :key k))
          (get-value-info (assoc arg
                                 :crdt (get-in crdt [:children k])
                                 :norm-path (conj norm-path k)
                                 :path (or ks [])
                                 :schema (get-child-schema k))))))))

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

(defmethod get-value-info :single-value
  [{:keys [norm-path path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT." arg))
    {:value (get-single-value arg)
     :norm-path norm-path}))

(defmethod get-value-info :map
  [{:keys [schema] :as arg}]
  (let [get-child-schema #(l/schema-at-path schema [%])]
    (associative-get-value-info (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value-info :record
  [{:keys [schema path] :as arg}]
  (let [get-child-schema (fn [k]
                           (or (l/schema-at-path schema [k])
                               (throw (ex-info (str "Bad record key `" k
                                                    "` in path `"
                                                    path "`.")
                                               (u/sym-map path k)))))]
    (associative-get-value-info (assoc arg :get-child-schema get-child-schema))))

(defn edn-schema->pred [edn-schema]
  (-> (lu/get-avro-type edn-schema)
      (lu/avro-type->pred)))

(defn get-union-branch-and-schema-for-value [{:keys [schema v]}]
  (let [member-schemas (vec (l/member-schemas schema))
        last-union-branch (dec (count member-schemas))]
    (loop [union-branch 0]
      (let [member-schema (nth member-schemas union-branch)
            edn-member-schema (l/edn member-schema)
            pred (edn-schema->pred edn-member-schema)]
        (cond
          (pred v)
          (u/sym-map union-branch member-schema)

          (= last-union-branch union-branch)
          (throw (ex-info
                  (str "The value `" v "` does not match any of the "
                       "member schemas in the union schema.")
                  {:union-edn-schema (l/edn schema)
                   :v v}))

          :else
          (recur (inc union-branch)))))))

(defn avro-type->key-pred [type]
  (case type
    :array integer?
    :map string?
    :record keyword?
    (throw (ex-info (str "The given avro type `" type "` is not a container.")
                    (u/sym-map type)))))

(defn get-union-branch-and-schema-for-key [{:keys [schema k]}]
  (let [member-schemas (vec (l/member-schemas schema))
        last-union-branch (dec (count member-schemas))]
    (loop [union-branch 0]
      (let [member-schema (nth member-schemas union-branch)
            avro-type (l/schema-type member-schema)
            container? (lu/avro-container-types avro-type)
            pred (when container?
                   (avro-type->key-pred avro-type))]
        (cond
          (and container? (pred k))
          (u/sym-map union-branch member-schema)

          (= last-union-branch union-branch)
          (throw (ex-info
                  (str "The path key `" k "` does not match any of the "
                       "member schemas in the union schema.")
                  {:union-edn-schema (l/edn schema)
                   :k k}))

          :else
          (recur (inc union-branch)))))))

(defn get-member-schema [{:keys [crdt path schema]}]
  (if-let [branch (:union-branch crdt)]
    (l/member-schema-at-branch schema branch)
    (:member-schema (get-union-branch-and-schema-for-key {:k (first path)
                                                          :schema schema}))))

(defmethod get-value-info :union
  [{:keys [crdt path] :as arg}]
  (if (empty? crdt)
    {:norm-path path
     :value nil}
    (let [member-schema (get-member-schema arg)]
      (get-value-info (assoc arg :schema member-schema)))))
