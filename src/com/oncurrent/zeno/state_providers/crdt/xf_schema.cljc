(ns com.oncurrent.zeno.state-providers.crdt.xf-schema
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti ->crdt-schema* (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmethod ->crdt-schema* :array
  [{:keys [schema] :as arg}]
  (let [children-schema (l/map-schema
                         (->crdt-schema*
                          (assoc arg :schema (l/child-schema schema))))
        sch-name (->> (l/fingerprint128 schema)
                      (ba/byte-array->b16-alpha-str)
                      (str "array-crdt-")
                      (keyword (namespace ::foo)))
        fields [[:children children-schema]
                [:container-add-ids l/string-set-schema]
                [:deleted-node-ids l/string-set-schema]
                [:node-id-to-child-infos (l/map-schema
                                          (l/array-schema
                                           shared/crdt-array-node-info-schema))]
                [:ordered-node-ids (l/array-schema shared/node-id-schema)]]]
    (l/record-schema sch-name fields)))

(defmethod ->crdt-schema* :map
  [{:keys [schema] :as arg}]
  (let [children-schema (l/map-schema
                         (->crdt-schema*
                          (assoc arg :schema (l/child-schema schema))))
        sch-name (->> (l/fingerprint128 schema)
                      (ba/byte-array->b16-alpha-str)
                      (str "map-crdt-")
                      (keyword (namespace ::foo)))
        fields [[:children children-schema]
                [:container-add-ids l/string-set-schema]
                [:deleted-container-add-ids l/string-set-schema]]]
    (l/record-schema sch-name fields)))

(defmethod ->crdt-schema* :record
  [{:keys [schema] :as arg}]
  (let [edn (l/edn schema)
        children-schema (l/record-schema
                         (keyword (namespace ::foo)
                                  (str (name (:name edn)) "-crdt"))
                         (mapv (fn [{field :name}]
                                 [field (->crdt-schema*
                                         (assoc arg :schema
                                                (l/child-schema
                                                 schema field)))])
                               (:fields edn)))
        sch-name (->> (l/fingerprint128 schema)
                      (ba/byte-array->b16-alpha-str)
                      (str "record-crdt-")
                      (keyword (namespace ::foo)))
        fields [[:children children-schema]
                [:container-add-ids l/string-set-schema]
                [:deleted-container-add-ids l/string-set-schema]]]
    (l/record-schema sch-name fields)))

(defmethod ->crdt-schema* :single-value
  [{:keys [schema] :as arg}]
  (let [fp-str (->> (l/fingerprint128 schema)
                    (ba/byte-array->b16-alpha-str))
        aitvi-schema (l/map-schema
                      (l/record-schema
                       (keyword (namespace ::foo) (str "value-info-" fp-str))
                       [[:value schema]
                        [:sys-time-ms schemas/timestamp-ms-schema]]))
        sch-name (->> (l/fingerprint128 schema)
                      (ba/byte-array->b16-alpha-str)
                      (str "value-crdt-")
                      (keyword (namespace ::foo)))
        fields [[:add-id-to-value-info aitvi-schema]
                [:deleted-add-ids l/string-set-schema]]]
    (l/record-schema sch-name fields)))

(defmethod ->crdt-schema* :union
  [{:keys [schema] :as arg}]
  (let [sch-name (->> (l/fingerprint128 schema)
                      (ba/byte-array->b16-alpha-str)
                      (str "union-crdt-")
                      (keyword (namespace ::foo)))
        member-schemas (l/member-schemas schema)
        fields (reduce
                (fn [acc union-branch]
                  (let [member-schema (nth member-schemas union-branch)
                        branch-k (-> (str "branch-" union-branch)
                                     (keyword))
                        branch-time-k (-> (str "branch-"  union-branch
                                               "-sys-time-ms")
                                          (keyword))]
                    (-> acc
                        (conj [branch-k (->crdt-schema*
                                         (assoc arg :schema member-schema))])
                        (conj [branch-time-k schemas/timestamp-ms-schema]))))
                []
                (range (count member-schemas)))]
    (l/record-schema sch-name fields)))

(defn ->crdt-schema [{:keys [data-schema] :as arg}]
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to "
                         "`->crdt-schema`. Got `" (or data-schema "nil") "`.")
                    (u/sym-map data-schema))))
  (->crdt-schema* (assoc arg :schema data-schema)))
