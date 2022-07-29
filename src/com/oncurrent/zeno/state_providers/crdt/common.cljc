(ns com.oncurrent.zeno.state-providers.crdt.common
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
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

(def array-head-node-id "__HEAD__")
(def container-types #{:array :map :record :union})
(def insert-after-cmd-types #{:zeno/insert-after
                              :zeno/insert-range-after})
(def insert-before-cmd-types #{:zeno/insert-before
                               :zeno/insert-range-before})
(def insert-cmd-types (set/union insert-after-cmd-types
                                 insert-before-cmd-types))
(def range-cmd-types #{:zeno/insert-range-after
                       :zeno/insert-range-before})
(def tx-info-prefix "_TX-INFO-FOR-TX-ID-")

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti check-key (fn [{:keys [schema]}]
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

(defn tx-id->tx-info-k [tx-id]
  (str tx-info-prefix tx-id))

(defn deserialize-op-value [arg]
  (throw (ex-info "Implement me." {})))

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

(defn make-update-state-tx-info-base
  [{:keys [actor-id client-id cmds make-tx-id]}]
  (let [tx-id (if make-tx-id
                (make-tx-id)
                (u/compact-random-uuid))
        sys-time-ms (u/current-time-ms)
        updated-paths (map :zeno/path cmds)]
    (u/sym-map actor-id client-id sys-time-ms tx-id updated-paths)))

(defn <get-tx-info [{:keys [storage tx-id]}]
  (let [k (tx-id->tx-info-k tx-id)]
    (storage/<get storage k shared/tx-info-schema)))

(defn <get-tx-infos [{:keys [storage tx-ids]}]
  (au/go
    (if (zero? (count tx-ids))
      []
      (let [<get (fn [tx-id]
                   (au/go
                     (let [info (au/<? (<get-tx-info
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

(defn <ba->snapshot [{:keys [ba schema storage] :as arg}]
  (au/go
    (when ba
      #_
      (let [serialized-value (l/deserialize-same schemas/serialized-value-schema
                                                 ba)
            sv->v-arg (assoc arg
                             :reader-schema shared/serializable-snapshot-schema
                             :serialized-value serialized-value)
            ser-snap (au/<? (common/<serialized-value->value sv->v-arg))
            crdt-info (edn/read-string (:edn-crdt-info ser-snap))]
        crdt-info))))

(defn <get-snapshot-from-url [{:keys [url] :as arg}]
  (au/go
    (when url
      (let [ba (au/<? (u/<http-get {:url url}))]
        (au/<? (<ba->snapshot (assoc arg :ba ba)))))))
