(ns oncurrent.zeno.crdt
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))


(def array-end-add-id "-END-")
(def array-start-add-id "-START-")
(def container-types #{:map :array :record})
(def crdt-value-info-keys [:sys-time-ms :union-branch :value])

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti apply-op (fn [{:keys [op-type schema]}]
                     [(schema->dispatch-type schema) op-type]))

(defmulti get-value (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmulti check-key (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

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

(defn same-cv-info? [x y]
  (and x
       y
       (= (select-keys x crdt-value-info-keys)
          (select-keys y crdt-value-info-keys))))

;; TODO: Replace this with conflict resolution
(defn get-most-recent [current-add-id-to-value-info]
  (->> (vals current-add-id-to-value-info)
       (sort-by :sys-time-ms)
       (last)))

(defn get-single-value [{:keys [crdt schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (when (pos? (count current-add-id-to-value-info))
      (let [schema-type (l/schema-type schema)
            value-info (get-most-recent current-add-id-to-value-info)
            {:keys [value union-branch]} value-info]
        (if-not (and (= :union schema-type) union-branch)
          value
          (let [member-schema (-> (l/edn schema)
                                  (nth union-branch)
                                  (l/edn->schema))
                member-schema-type (l/schema-type member-schema)]
            (if-not (container-types member-schema-type)
              value
              (get-value (assoc arg :schema member-schema)))))))))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt op-type path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map add-id crdt path)))
    (let [{:keys [current-add-id-to-value-info deleted-add-ids]} crdt
          cur-value-info (get current-add-id-to-value-info add-id)
          value-info (select-keys arg crdt-value-info-keys)
          same? (same-cv-info? cur-value-info value-info)
          deleted? (get deleted-add-ids add-id)]
      (when (and value-info cur-value-info (not same?))
        (throw (ex-info
                (str "Attempt to reuse an existing add-id "
                     "(`" add-id "`) to add different value info to CRDT.")
                (u/sym-map add-id cur-value-info op-type value-info))))
      (if (or deleted? same?)
        crdt
        (update crdt :current-add-id-to-value-info assoc add-id value-info)))))

(defmethod apply-op [:single-value :delete-value]
  [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-id-to-value-info dissoc add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defn associative-add-key
  [{:keys [add-id crdt key op-type path schema] :as arg}]
  (if (seq path)
    (let [[child-key & ks] path
          child-schema (l/schema-at-path schema [child-key])]
      (check-key (assoc arg :key child-key))
      (update-in crdt [:children child-key]
                 (fn [child-crdt]
                   (apply-op (assoc arg
                                    :crdt child-crdt
                                    :path ks
                                    :schema child-schema)))))
    (let [{:keys [current-add-id-to-key deleted-add-ids]} crdt
          cur-key (get current-add-id-to-key add-id)
          _ (when (and cur-key key (not= cur-key key))
              (throw (ex-info
                      (str "Attempt to reuse an existing add-id "
                           "(`" add-id "`) to add different key to CRDT.")
                      (u/sym-map add-id crdt key op-type))))
          deleted? (get deleted-add-ids add-id)]
      (check-key arg)
      (if deleted?
        crdt
        (update crdt :current-add-id-to-key assoc add-id key)))))

(defmethod apply-op [:map :add-key]
  [{:keys [add-id key op-type path] :as arg}]
  (associative-add-key arg))

(defn associative-delete-key
  [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-id-to-key dissoc add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defmethod apply-op [:map :delete-key]
  [arg]
  (associative-delete-key arg))

(defmethod apply-op [:record :delete-key]
  [arg]
  (associative-delete-key arg))

(defmethod apply-op [:record :add-key]
  [{:keys [add-id key op-type path] :as arg}]
  (associative-add-key arg))

(defn associative-add-value
  [{:keys [add-id crdt path schema throw-not-associative value]
    :as arg}]
  (let [root? (empty? path)]
    (cond
      (and root? (not (associative? value)))
      (throw-not-associative)

      root?
      (reduce-kv (fn [crdt* k v]
                   (check-key (assoc arg :key k))
                   (let [child-schema (l/schema-at-path schema [k])]
                     (update-in crdt* [:children k]
                                (fn [child-crdt]
                                  (apply-op (assoc arg
                                                   :crdt child-crdt
                                                   :path nil
                                                   :schema child-schema
                                                   :value v))))))
                 crdt
                 value)

      :else
      (let [[k & ks] path
            child-schema (l/schema-at-path schema [k])]
        (check-key (assoc arg :key k))
        (update-in crdt [:children k]
                   (fn [child-crdt]
                     (apply-op (assoc arg
                                      :crdt child-crdt
                                      :path ks
                                      :schema child-schema))))))))

(defmethod apply-op [:map :add-value]
  [{:keys [add-id path value] :as arg}]
  (let [throw-not-associative #(throw
                                (ex-info
                                 (str
                                  "Path indicates a map, but value is not "
                                  "associative. Got: `" (or value "nil") "`.")
                                 (u/sym-map add-id path value)))]
    (associative-add-value
     (assoc arg :throw-not-associative throw-not-associative))))

(defn associative-delete-value
  [{:keys [add-id child-keys crdt path schema] :as arg}]
  (if (empty? path)
    (let [new-crdt (update crdt :deleted-add-ids (fn [ids]
                                                   (if (seq ids)
                                                     (conj ids add-id)
                                                     #{add-id})))]
      (reduce (fn [crdt* k]
                (check-key (assoc arg :key k))
                (apply-op (assoc arg
                                 :crdt crdt*
                                 :path [k])))
              new-crdt
              child-keys))
    (let [[k & ks] path
          child-schema (l/schema-at-path schema [k])]
      (update-in crdt [:children k]
                 (fn [child-crdt]
                   (apply-op (assoc arg
                                    :crdt child-crdt
                                    :path ks
                                    :schema child-schema)))))))

(defmethod apply-op [:map :delete-value]
  [{:keys [children] :as arg}]
  (let [child-keys (keys children)]
    (associative-delete-value (assoc arg :child-keys child-keys))))

(defmethod apply-op [:record :add-value]
  [{:keys [add-id path schema value] :as arg}]
  (let [throw-not-associative #(throw
                                (ex-info
                                 (str
                                  "Path indicates a record, but value is not "
                                  "associative. Got: `" (or value "nil") "`.")
                                 (u/sym-map add-id path value)))
        fields (->> (l/edn schema)
                    (:fields)
                    (map :name))
        value* (if (associative? value)
                 (select-keys value fields)
                 value)]
    (associative-add-value
     (assoc arg
            :check-key check-key
            :throw-not-associative throw-not-associative
            :value value*))))

(defmethod apply-op [:record :delete-value]
  [{:keys [schema] :as arg}]
  (let [child-keys (->> (l/edn schema)
                        (:fields)
                        (map :name))]
    (associative-delete-value (assoc arg :child-keys child-keys))))

(defn apply-ops [{:keys [crdt ops schema sys-time-ms]}]
  (reduce (fn [crdt* op]
            (apply-op (assoc op
                             :crdt crdt*
                             :schema schema
                             :sys-time-ms sys-time-ms)))
          crdt
          ops))

(defmethod get-value :single-value
  [{:keys [path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT." arg))
    (get-single-value arg)))

(defn associative-get-value
  [{:keys [check-child-k crdt path] :as arg}]
  (if (empty? path)
    (reduce-kv (fn [acc add-id k]
                 (let [v (get-value (assoc arg
                                           :path [k]))]
                   (if (or (nil? v)
                           (and (coll? v) (empty? v)))
                     acc
                     (assoc acc k v))))
               {}
               (:current-add-id-to-key crdt))
    (let [[k & ks] path]
      (check-child-k k)
      (get-value (assoc arg
                        :crdt (get-in crdt [:children k])
                        :schema (l/schema-at-path (:schema arg) [k])
                        :path ks)))))

(defmethod get-value :map
  [{:keys [path] :as arg}]
  (let [check-child-k (fn [k]
                        (when-not (string? k)
                          (throw (ex-info (str "Map key in path is not a string"
                                               ". Got: ` "(or k "nil") "`.")
                                          (u/sym-map k path)))))]
    (associative-get-value (assoc arg :check-child-k check-child-k))))

(defmethod get-value :record
  [{:keys [path] :as arg}]
  (let [check-child-k (fn [k]
                        (when-not (keyword? k)
                          (throw (ex-info
                                  (str "Record key in path is not a keyword. "
                                       "Got :`" (or k "nil") "`.")
                                  (u/sym-map k path)))))]
    (associative-get-value (assoc arg :check-child-k check-child-k))))
