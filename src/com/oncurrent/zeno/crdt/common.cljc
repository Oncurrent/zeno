(ns com.oncurrent.zeno.crdt.common
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn chop-root [path]
  (case (first path)
    :zeno/crdt (recur (-> path rest))
    path))

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
    (associative-get-value-info
     (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value-info :record
  [{:keys [schema path] :as arg}]
  (let [get-child-schema (fn [k]
                           (or (l/schema-at-path schema [k])
                               (throw (ex-info (str "Bad record key `" k
                                                    "` in path `"
                                                    path "`.")
                                               (u/sym-map path k)))))]
    (associative-get-value-info
     (assoc arg :get-child-schema get-child-schema))))

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

(defn get-op-value-schema [{:keys [op-type norm-path crdt-schema]}]
  (case op-type
    :add-array-edge schemas/crdt-array-edge-schema
    :add-value (l/schema-at-path crdt-schema norm-path)
    :delete-array-edge nil
    :delete-value nil))

(defn <crdt-op->serializable-crdt-op
  [{:keys [storage crdt-schema op]}]
  (au/go
    (when op
      (let [{:keys [norm-path op-type path value]} op
            schema (get-op-value-schema
                    (u/sym-map op-type norm-path crdt-schema))]
        (cond-> op
          true (dissoc :path)
          true (dissoc :value)
          schema (assoc :serialized-value
                        (au/<? (storage/<value->serialized-value
                                storage schema value))))))))

(defn <serializable-crdt-op->crdt-op
  [{:keys [storage crdt-schema op] :as arg}]
  (au/go
    (when op
      (let [{:keys [norm-path op-type serialized-value]} op
            schema (get-op-value-schema
                    (u/sym-map op-type norm-path crdt-schema))]
        (cond-> op
          true (dissoc :serialized-value)
          schema (assoc :value
                        (au/<? (common/<serialized-value->value
                                (assoc arg
                                       :reader-schema schema
                                       :serialized-value serialized-value)))))))))

(defn <crdt-ops->serializable-crdt-ops [arg]
  (au/go
    (let [ops (seq (:ops arg))
          last-i (count ops)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [op (nth ops i)
                s-op (au/<? (<crdt-op->serializable-crdt-op
                             (assoc arg :op op)))
                new-i (inc i)
                new-out (conj out s-op)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))

(defn <serializable-crdt-ops->crdt-ops [arg]
  (au/go
    (let [ops (seq (:ops arg))
          last-i (count ops)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [s-op (nth ops i)
                op (au/<? (<serializable-crdt-op->crdt-op
                           (assoc arg :op s-op)))
                new-i (inc i)
                new-out (conj out op)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))

(defn <update-info->serializable-update-info
  [{:keys [storage crdt-schema update-info]}]
  (au/go
    (when update-info
      (let [{:keys [norm-path value]} update-info
            value-schema (when-not (nil? value)
                           (l/schema-at-path crdt-schema
                                             (chop-root norm-path)))
            range? (#{:zeno/insert-range-after
                      :zeno/insert-range-before} (:op update-info))
            range-schema (when range?
                           (l/array-schema value-schema))]
        (cond-> (dissoc update-info :value)
          (not (nil? value)) (assoc :serialized-value
                                    (au/<? (storage/<value->serialized-value
                                            storage
                                            (if range?
                                              range-schema
                                              value-schema)
                                            value))))))))

(defn <serializable-update-info->update-info
  [{:keys [crdt-schema update-info] :as arg}]
  (au/go
    (when update-info
      (let [{:keys [norm-path serialized-value]} update-info
            value-schema (l/schema-at-path crdt-schema (chop-root norm-path))]
        (-> update-info
            (dissoc :serialized-value)
            (assoc :value
                   (au/<? (common/<serialized-value->value
                           (assoc arg
                                  :reader-schema value-schema
                                  :serialized-value serialized-value)))))))))

(defn <update-infos->serializable-update-infos [{:keys [update-infos] :as arg}]
  (au/go
    (let [infos (seq update-infos)
          last-i (count infos)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [info (nth infos i)
                s-info (au/<? (<update-info->serializable-update-info
                               (assoc arg :update-info info)))
                new-i (inc i)
                new-out (conj out s-info)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))

(defn <serializable-update-infos->update-infos [arg]
  (au/go
    (let [infos (seq (:update-infos arg))
          last-i (count infos)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [s-info (nth infos i)
                info (au/<? (<serializable-update-info->update-info
                             (assoc arg :update-info s-info)))
                new-i (inc i)
                new-out (conj out info)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))
