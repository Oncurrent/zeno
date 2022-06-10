(ns com.oncurrent.zeno.state-providers.crdt.common
  (:require
   [clojure.core.async :as ca]
   [clojure.edn :as edn]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [com.oncurrent.zeno.common :as common]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.storage :as storage]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:array :map :record :union})
(def tx-info-prefix "_TX-INFO-FOR-TX-ID-")

(defn tx-id->tx-info-k [tx-id]
  (str tx-info-prefix tx-id))

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti check-key (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

;; Returns a map w/ `:norm-path` and `:value`
(defmulti get-value-info (fn [{:keys [schema]}]
                           (schema->dispatch-type schema)))

(defmulti repair (fn [{:keys [schema]}]
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
    (let [{:keys [children container-add-ids]} crdt
          exists? (seq container-add-ids)
          value (when exists?
                  (reduce-kv
                   (fn [acc k child-crdt]
                     (let [vi (get-value-info
                               (assoc arg :path [k]))]
                       (if (:exists? vi)
                         (assoc acc k (:value vi))
                         acc)))
                   {}
                   children))]
      (u/sym-map exists? value norm-path))
    (let [[k & ks] path]
      (check-key (assoc arg :key k))
      (let [child-crdt (get-in crdt [:children k])
            child-schema (get-child-schema k)]
        (get-value-info (assoc arg
                               :crdt child-crdt
                               :norm-path (conj (or norm-path []) k)
                               :path (or ks [])
                               :schema child-schema))))))

(defmethod get-value-info :single-value
  [{:keys [norm-path path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map path norm-path)))
    (let [vi (some-> arg :crdt :current-add-id-to-value-info first val)]
      {:value (:value vi)
       :exists? (boolean vi)
       :norm-path norm-path})))

(defmethod get-value-info :map
  [{:keys [schema] :as arg}]
  (let [get-child-schema (fn [_] (l/child-schema schema))]
    (associative-get-value-info
     (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value-info :record
  [{:keys [schema path] :as arg}]
  (let [get-child-schema (fn [k]
                           (or (l/child-schema schema k)
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
  (let [branch (:union-branch crdt)]
    ;; TODO: These cond branches are all about getting reads to work properly.
    ;; See for example unit.crdt-commands-test/test-set-empty-record-with-map-of-records
    ;; You need to be able to read a nested path when the root of the path is
    ;; nil. You need to be able to pass in nil as a key to e.g. a map in case
    ;; an early subscription returned nil then you used it as a key in the next
    ;; one. The todo here is figure if there's a more elegant way then these
    ;; rather hard coded handlings of what you get in the above cases.
    (cond
      (and branch (empty? path)) (l/member-schema-at-branch schema branch)
      (empty? path) nil
      (and (not (empty? path)) (nil? (first path))) nil
      :else (:member-schema (get-union-branch-and-schema-for-key
                             {:k (first path)
                              :schema schema})))))

(defmethod get-value-info :union
  [{:keys [crdt norm-path path schema] :as arg}]
  (if (empty? crdt)
    {:exists? false
     :norm-path norm-path
     :value nil}
    (if-let [member-schema (get-member-schema arg)]
      (get-value-info (assoc arg :schema member-schema))
      {:exists? false
       :norm-path norm-path
       :value nil})))

(defn get-op-value-info [{:keys [op-type schema op-path]}]
  (case op-type
    :add-array-edge {:value-schema shared/crdt-array-edge-schema}
    :add-container nil
    :add-value {:value-schema (l/schema-at-path schema op-path
                                                {:branches? true})}
    :delete-array-edge nil
    :delete-container {:ser-v->v-xf set
                       :v->ser-v-xf seq
                       :value-schema (l/array-schema shared/add-id-schema)}
    :delete-value nil))

(defn <crdt-op->serializable-crdt-op
  [{:keys [storage schema crdt-op]}]
  (au/go
    (when crdt-op
      (let [{:keys [op-path op-type value]} crdt-op
            {:keys [v->ser-v-xf]
             w-schema :value-schema} (get-op-value-info
                                      (u/sym-map op-type op-path schema))
            v (cond-> value
                v->ser-v-xf (v->ser-v-xf value))]
        (cond-> crdt-op
          true (dissoc :value)
          w-schema (assoc :serialized-value
                          (au/<? (storage/<value->serialized-value
                                  storage w-schema v))))))))

(defn <serializable-crdt-op->crdt-op
  [{:keys [storage schema crdt-op] :as arg}]
  (au/go
    (when crdt-op
      (let [{:keys [op-path op-type serialized-value]} crdt-op
            {:keys [ser-v->v-xf]
             r-schema :value-schema} (get-op-value-info
                                      (u/sym-map op-type op-path schema))]
        (cond-> crdt-op
          true (dissoc :serialized-value)
          r-schema (assoc :value
                          (au/<?
                           (common/<serialized-value->value
                            (assoc arg
                                   :reader-schema r-schema
                                   :serialized-value serialized-value))))
          ser-v->v-xf (update :value ser-v->v-xf))))))

(defn <crdt-ops->serializable-crdt-ops [arg]
  (au/go
    (let [crdt-ops (vec (:crdt-ops arg))
          last-i (count crdt-ops)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [crdt-op (nth crdt-ops i)
                s-op (au/<? (<crdt-op->serializable-crdt-op
                             (assoc arg :crdt-op crdt-op)))
                new-i (inc i)
                new-out (conj out s-op)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))

(defn <serializable-crdt-ops->crdt-ops [arg]
  (au/go
    (let [crdt-ops (vec (:crdt-ops arg))
          last-i (count crdt-ops)]
      (if (zero? last-i)
        []
        (loop [i 0
               out []]
          (let [s-op (nth crdt-ops i)
                crdt-op (au/<? (<serializable-crdt-op->crdt-op
                                (assoc arg :crdt-op s-op)))
                new-i (inc i)
                new-out (conj out crdt-op)]
            (if (= last-i new-i)
              new-out
              (recur new-i new-out))))))))

(defn <tx-info->serializable-tx-info
  [{:keys [tx-info] :as arg}]
  (au/go
    (let [ser-crdt-ops (au/<? (<crdt-ops->serializable-crdt-ops
                               (assoc arg :crdt-ops (:crdt-ops tx-info))))]
      (assoc tx-info :crdt-ops ser-crdt-ops))))

(defn <serializable-tx-info->tx-info
  [{:keys [serializable-tx-info storage] :as fn-arg}]
  (au/go
    (let [crdt-ops (au/<? (<serializable-crdt-ops->crdt-ops
                           (assoc fn-arg
                                  :crdt-ops (:crdt-ops serializable-tx-info))))]
      (assoc serializable-tx-info :crdt-ops crdt-ops))))

(defn <serializable-tx-infos->tx-infos
  [{:keys [serializable-tx-infos] :as fn-arg}]
  (au/go
    (if (empty? serializable-tx-infos)
      []
      (let [last-i (dec (count serializable-tx-infos))
            serializable-tx-infos* (vec serializable-tx-infos)]
        (loop [i 0
               out []]
          (let [serializable-tx-info (nth serializable-tx-infos* i)
                tx-info (au/<? (<serializable-tx-info->tx-info
                                (assoc fn-arg :serializable-tx-info
                                       serializable-tx-info)))
                new-out (conj out tx-info)]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defn <get-serializable-tx-info [{:keys [storage tx-id]}]
  (let [k (tx-id->tx-info-k tx-id)]
    (storage/<get storage k shared/serializable-tx-info-schema)))

(defn <get-serializable-tx-infos [{:keys [storage tx-ids]}]
  (au/go
    (if (zero? (count tx-ids))
      []
      (let [<get (fn [tx-id]
                   (au/go
                     (let [info (au/<? (<get-serializable-tx-info
                                        (u/sym-map storage tx-id)))]
                       [tx-id info])))
            chs (map <get tx-ids)
            ret-ch (ca/merge chs)
            last-i (dec (count tx-ids))]
        (loop [i 0
               out {}]
          (let [ret (au/<? ret-ch)
                new-out (if ret
                          (assoc out (first ret) (second ret))
                          out)]
            (if (not= last-i i)
              (recur (inc i) new-out)
              (reduce (fn [acc tx-id]
                        (if-let [info (get new-out tx-id)]
                          (conj acc info)
                          (throw (ex-info (str "tx-info for tx-id `" tx-id
                                               "` was not found in storage.")
                                          (u/sym-map tx-id)))))
                      []
                      tx-ids))))))))

(defn get-value [{:keys [crdt make-id path norm-path schema] :as arg}]
  (-> (get-value-info (assoc arg :norm-path (or norm-path [])))
      (:value)))

(defn <ba->snapshot [{:keys [ba schema storage] :as arg}]
  (au/go
    (when ba
      (let [serialized-value (l/deserialize-same schemas/serialized-value-schema
                                                 ba)
            sv->v-arg (assoc arg
                             :reader-schema shared/serializable-snapshot-schema
                             :serialized-value serialized-value)
            ser-snap (au/<? (common/<serialized-value->value sv->v-arg))
            crdt (edn/read-string (:edn-crdt ser-snap))]
        (u/sym-map crdt)))))

(defn <get-snapshot-from-url [{:keys [url] :as arg}]
  (au/go
    (when url
      (let [ba (au/<? (u/<http-get {:url url}))]
        (au/<? (<ba->snapshot (assoc arg :ba ba)))))))

(defn make-update-state-tx-info-base
  [{:keys [actor-id client-id cmds make-tx-id]}]
  (let [tx-id (if make-tx-id
                (make-tx-id)
                (u/compact-random-uuid))
        sys-time-ms (u/current-time-ms)
        updated-paths (map :zeno/path cmds)]
    (u/sym-map actor-id client-id sys-time-ms tx-id updated-paths)))

(defn xf-op-paths [{:keys [prefix crdt-ops]}]
  (reduce (fn [acc op]
            (conj acc (update op :op-path #(cons prefix %))))
          #{}
          crdt-ops))
