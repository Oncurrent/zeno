(ns com.oncurrent.zeno.state-providers.crdt.xf-schema
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti ->crdt-schema* (fn [{:keys [data-schema]}]
                           (c/schema->dispatch-type data-schema)))

(defn ->crdt-schema-impl
  [{:keys [array-fields children-schema value-schema] :as arg}]
  (let [fp-str (-> (or children-schema value-schema)
                   (l/fingerprint128)
                   (ba/byte-array->hex-str))
        aitvi-schema (when value-schema
                       (l/map-schema
                        (l/record-schema
                         (keyword (namespace ::foo) (str "value-info-" fp-str))
                         [[:value value-schema]
                          [:sys-time-ms schemas/timestamp-ms-schema]])))
        fields (cond
                 array-fields array-fields
                 children-schema [[:children children-schema]
                                  [:container-add-ids l/string-set-schema]
                                  [:deleted-container-add-ids
                                   l/string-set-schema]]
                 value-schema [[:add-id-to-value-info aitvi-schema]
                               [:deleted-add-ids l/string-set-schema]]
                 :else (throw (ex-info "Missing field info." {})))]
    (l/record-schema (keyword (namespace ::foo) (str "crdt-info-" fp-str))
                     fields)))

(defmethod ->crdt-schema* :array
  [{:keys [data-schema] :as arg}]
  (let [children-schema (l/map-schema (->crdt-schema*
                                       {:data-schema (l/child-schema
                                                      data-schema)}))
        fields [[:add-id-to-edge shared/crdt-array-edge-schema]
                [:head-node-id-to-edge (l/map-schema
                                        shared/crdt-array-edge-schema)]
                [:deleted-edges (l/array-schema shared/crdt-array-edge-schema)]
                [:ordered-node-ids l/string-set-schema]]]
    (->crdt-schema-impl {:array-fields fields
                         :children-schema children-schema})))

(defmethod ->crdt-schema* :map
  [{:keys [data-schema] :as arg}]
  (let [children-schema (l/map-schema (->crdt-schema*
                                       {:data-schema (l/child-schema
                                                      data-schema)}))]
    (->crdt-schema-impl (u/sym-map children-schema))))

(defmethod ->crdt-schema* :record
  [{:keys [data-schema] :as arg}]
  (let [{:keys [fields] :as edn} (l/edn data-schema)
        children-schema (l/record-schema
                         (keyword (namespace ::foo)
                                  (str (name (:name edn)) "-crdt-info"))
                         (mapv (fn [{field :name}]
                                 [field (->crdt-schema*
                                         {:data-schema
                                          (l/child-schema data-schema field)})])
                               fields))]
    (->crdt-schema-impl (u/sym-map children-schema))))

(defmethod ->crdt-schema* :single-value
  [arg]
  (->crdt-schema-impl {:value-schema (:data-schema arg)}))

(defmethod ->crdt-schema* :union
  [{:keys [data-schema] :as arg}]
  (let [container-schemas (reduce
                           (fn [acc member-schema]
                             (if (= :single-value (c/schema->dispatch-type
                                                   member-schema))
                               acc
                               (conj acc member-schema)))
                           []
                           (l/member-schemas data-schema))
        children-schemas (some->> container-schemas
                                  (map #(->crdt-schema* {:data-schema %}))
                                  (map l/edn)
                                  (set)
                                  (map l/edn->schema))
        children-schema (when (seq children-schemas)
                          (l/union-schema children-schemas))]
    (->crdt-schema-impl (u/sym-map children-schema))))

(defn ->crdt-schema [{:keys [data-schema] :as arg}]
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to "
                         "`->crdt-schema`. Got `" (or data-schema "nil") "`.")
                    (u/sym-map data-schema))))
  (->crdt-schema* arg))
