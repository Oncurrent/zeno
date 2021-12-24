(ns oncurrent.zeno.crdts
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn make-map-keyset-crdt-key [item-id]
  (str storage/map-keyset-crdt-key-prefix item-id))

(defn make-map-key-value-crdt-key [item-id k]
  (str storage/map-key-value-crdt-key-prefix item-id "-" k))

(defn make-record-key-value-crdt-key [item-id k]
  (when-not (keyword? k)
    (throw (ex-info (str "Record key must be a keyword. Got: `" k "`.")
                    (u/sym-map k item-id))))
  (str storage/record-key-value-crdt-key-prefix item-id "-"
       (namespace k) "-" (name k)))

(defn <add-to-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/set-crdt-schema
                  (fn [crdt]
                    (if (-> (:deleted-add-ids crdt)
                            (get add-id))
                      crdt
                      (update crdt :current-value-infos conj arg)))))

(defn <del-from-crdt! [{:keys [add-id crdt-key storage]}]
  (storage/<swap! storage crdt-key schemas/set-crdt-schema
                  (fn [crdt]
                    (let [new-cvis (reduce
                                    (fn [acc cvi]
                                      (if (= add-id (:add-id cvi))
                                        acc
                                        (conj acc cvi)))
                                    []
                                    (:current-value-infos crdt))]
                      (-> crdt
                          (assoc :current-value-infos new-cvis)
                          (update :deleted-add-ids (fn [dai]
                                                     (if (seq dai)
                                                       (conj dai add-id)
                                                       #{add-id}))))))))

(defmulti <apply-op! #(-> % :op :op-type))

(defmethod <apply-op! :add-record-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key item-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-record-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key item-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key item-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key item-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key
  [{:keys [storage op]}]
  (au/go
    (let [{:keys [item-id k]} op
          ser-k (au/<? (storage/<value->serialized-value
                        storage l/string-schema k))
          arg (assoc op
                     :crdt-key (make-map-keyset-crdt-key item-id)
                     :serialized-value ser-k
                     :storage storage)]
      (au/<? (<add-to-crdt! arg)))))

(defmethod <apply-op! :del-map-key
  [{:keys [storage op]}]
  (let [arg (assoc op
                   :crdt-key (make-map-keyset-crdt-key (:item-id op))
                   :storage storage)]
    (<del-from-crdt! arg)))

(defn <apply-ops!
  [{:keys [ops storage]}]
  (au/go
    (let [num-ops (count ops)
          <apply-op!* (fn [op]
                        (<apply-op! (u/sym-map op storage)))
          ch (ca/merge (map <apply-op!* ops))]
      (if (zero? num-ops)
        true
        (loop [num-done 0]
          (au/<? ch)
          (let [new-num-done (inc num-done)]
            (if (= num-ops new-num-done)
              true
              (recur new-num-done))))))))

(defn get-most-recent [candidates]
  (reduce (fn [acc candidate]
            (if (> (:sys-time-ms candidate) (:sys-time-ms acc))
              candidate
              acc))
          (first candidates)
          candidates))

(defn <get-single-value-crdt-val
  [{:keys [crdt-key schema storage] :as arg}]
  (au/go
    (let [crdt (au/<? (storage/<get storage crdt-key schemas/set-crdt-schema))
          {:keys [current-value-infos]} crdt
          num-values (count current-value-infos)]
      (when (pos? num-values)
        (->> (get-most-recent current-value-infos)
             (:serialized-value)
             (storage/<serialized-value->value storage schema)
             (au/<?))))))

(defmulti <get-crdt-val
  ;; TODO: Check that all necessary arg keys are present, including
  ;; those for conflict resolution. Might be best to have <get-crdt-val
  ;; be a regular fn which does the key checking, then calls
  ;; <get-crdt-val* (the multimethod).
  (fn [{:keys [item-id k schema] :as arg}]
    (let [schema-type (l/schema-type schema)]
      (cond
        (and k (= :map schema-type)) :map-kv
        (= :map schema-type) :map
        (and k (= :record schema-type)) :record-kv
        (= :record schema-type) :record
        :else (throw (ex-info "Could not determine target item type."
                              (u/sym-map item-id schema-type)))))))

(defmethod <get-crdt-val :map-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-map-key-value-crdt-key item-id k)]
    (<get-single-value-crdt-val (assoc arg
                                       :crdt-key crdt-key
                                       :schema (l/schema-at-path schema [k])))))

(defmethod <get-crdt-val :record-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-record-key-value-crdt-key item-id k)]
    (<get-single-value-crdt-val (assoc arg
                                       :crdt-key crdt-key
                                       :schema (l/schema-at-path schema [k])))))

(defmethod <get-crdt-val :map
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [keyset-k (make-map-keyset-crdt-key item-id)
          infos (some-> (storage/<get storage keyset-k schemas/set-crdt-schema)
                        (au/<?)
                        (:current-value-infos))
          num-infos (count infos)
          values-schema (l/schema-at-path schema ["x"])
          <get-kv (fn [{:keys [serialized-value]}]
                    (au/go
                      (let [k (au/<? (storage/<serialized-value->value
                                      storage
                                      l/string-schema
                                      serialized-value))
                            v (-> (assoc arg :k k)
                                  (<get-crdt-val)
                                  (au/<?))]
                        (u/sym-map k v))))
          ch (ca/merge (map <get-kv infos))]
      (if (zero? num-infos)
        {}
        (loop [out {}]
          (let [{:keys [k v] :as ret} (au/<? ch)
                new-out (assoc out k v)]
            (if (= num-infos (count new-out))
              new-out
              (recur new-out))))))))

(defmethod <get-crdt-val :record
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [field-ks (->> (l/edn schema)
                        (:fields)
                        (map :name))
          <get-kv (fn [k]
                    (au/go
                      (let [arg* (assoc arg :k k)
                            v (-> (assoc arg :k k)
                                  (<get-crdt-val)
                                  (au/<?))]
                        (u/sym-map k v))))
          ch (ca/merge (map <get-kv field-ks))
          num-fields (count field-ks)]
      (loop [fields-processed 0
             out {}]
        (let [{:keys [k v] :as ret} (au/<? ch)
              new-out (if (nil? v)
                        out
                        (assoc out k v))
              new-fields-processed (inc fields-processed)]
          (if (= num-fields new-fields-processed)
            new-out
            (recur new-fields-processed new-out)))))))
