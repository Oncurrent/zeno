(ns com.oncurrent.zeno.state-providers.crdt.xf-schema
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti ->crdt-info-schema (fn [{:keys [schema]}]
                               (c/schema->dispatch-type schema)))

(defmulti ->value-schema (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defn ->crdt-info-schema*
  [{:keys [schema children-schema]}]
  (let [fields (cond-> [[:add-id shared/add-id-schema]
                        [:sys-time-ms schemas/timestamp-ms-schema]]
                 children-schema (conj [:children children-schema]))
        fp-str (some-> children-schema
                       (l/fingerprint128)
                       (ba/byte-array->hex-str))]
    (l/record-schema (keyword (namespace ::foo) (str "crdt-info-" fp-str))
                     fields)))

(defmethod ->crdt-info-schema :array
  [{:keys [schema] :as arg}]
  (let [children-schema (l/map-schema (->crdt-info-schema
                                       {:schema (l/child-schema schema)}))]
    (->crdt-info-schema*
     (assoc arg :children-schema children-schema))))

(defmethod ->crdt-info-schema :map
  [{:keys [schema] :as arg}]
  (let [children-schema (l/map-schema (->crdt-info-schema
                                       {:schema (l/child-schema schema)}))]
    (->crdt-info-schema* (assoc arg :children-schema children-schema))))

(defmethod ->crdt-info-schema :record
  [{:keys [schema] :as arg}]
  (let [{:keys [fields] :as edn} (l/edn schema)
        children-schema (l/record-schema
                         (keyword (namespace ::foo)
                                  (str (name (:name edn)) "-crdt-info"))
                         (mapv (fn [{field :name}]
                                 [field (->crdt-info-schema
                                         {:schema
                                          (l/child-schema schema field)})])
                               fields))]
    (->crdt-info-schema* (assoc arg :children-schema children-schema))))

(defmethod ->crdt-info-schema :single-value
  [arg]
  (->crdt-info-schema* (dissoc arg :children-schema)))

(defmethod ->crdt-info-schema :union
  [{:keys [schema] :as arg}]
  (let [container-schemas (reduce
                           (fn [acc member-schema]
                             (if (= :single-value (c/schema->dispatch-type
                                                   member-schema))
                               acc
                               (conj acc member-schema)))
                           []
                           (l/member-schemas schema))
        children-schemas (some->> container-schemas
                                  (map #(->crdt-info-schema {:schema %}))
                                  (map l/edn)
                                  (set)
                                  (map l/edn->schema))
        children-schema (when (seq children-schemas)
                          (l/union-schema children-schemas))]
    (->crdt-info-schema* (assoc arg :children-schema children-schema))))

(defmethod ->value-schema :single-value
  [{:keys [schema]}]
  schema)

(defmethod ->value-schema :array
  [{:keys [schema]}]
  (let [{:keys [fields] :as edn} (l/edn schema)
        fp-str (-> (l/fingerprint128 schema)
                   (ba/byte-array->hex-str))]
    (l/record-schema
     (keyword (namespace ::foo) (str "array-value" fp-str))
     [[:add-id-to-edge shared/crdt-array-edge-schema]
      [:container-add-ids l/string-set-schema]
      [:head-node-id-to-edge (l/map-schema shared/crdt-array-edge-schema)]
      [:deleted-edges (l/array-schema shared/crdt-array-edge-schema)]
      [:node-id-to-value (l/map-schema (l/child-schema schema))]
      [:ordered-node-ids l/string-set-schema]])))

(defmethod ->value-schema :map
  [{:keys [schema]}]
  (l/map-schema (->value-schema {:schema (l/child-schema schema)})))

(defmethod ->value-schema :record
  [{:keys [schema]}]
  (let [{:keys [fields] :as edn} (l/edn schema)]
    (l/record-schema
     (keyword (namespace ::foo) (str (name (:name edn)) "-value"))
     (mapv (fn [{field :name}]
             [field (->value-schema {:schema (l/child-schema schema field)})])
           fields))))

(defmethod ->value-schema :union
  [{:keys [schema]}]
  (l/union-schema (mapv #(->value-schema {:schema %})
                        (l/member-schemas schema))))

(defn ->crdt-schema [{:keys [schema] :as arg}]
  (let [fp-str (-> (l/fingerprint128 schema)
                   (ba/byte-array->hex-str))]
    (l/record-schema
     (keyword (namespace ::foo) (str "crdt-" fp-str))
     [[:applied-tx-ids l/string-set-schema]
      [:crdt-info (->crdt-info-schema arg)]
      [:graveyard l/string-set-schema]
      [:value (->value-schema arg)]])))
