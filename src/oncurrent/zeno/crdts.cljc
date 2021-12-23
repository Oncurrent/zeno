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

(defn make-map-keyset-crdt-key [map-id]
  (str storage/map-keyset-crdt-key-prefix map-id))

(defn make-map-key-value-crdt-key [map-id k]
  (str storage/map-key-value-crdt-key-prefix map-id "-" k))

(defn make-record-key-value-crdt-key [record-id k]
  (when-not (keyword? k)
    (throw (ex-info (str "Record key must be a keyword. Got: `" k "`.")
                    (u/sym-map k record-id))))
  (str storage/record-key-value-crdt-key-prefix record-id "-"
       (namespace k) "-" (name k)))

(defn <add-to-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/crdt-schema
                  (fn [crdt]
                    (if (-> (:deleted-add-ids crdt)
                            (get add-id))
                      crdt
                      (update crdt :current-value-infos conj arg)))))

(defn <del-from-crdt! [{:keys [add-id crdt-key storage]}]
  (storage/<swap! storage crdt-key schemas/crdt-schema
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
  (let [{:keys [record-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key record-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-record-key-value
  [{:keys [storage op]}]
  (let [{:keys [record-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key record-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [map-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key map-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [map-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key map-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key
  [{:keys [storage op]}]
  (au/go
    (let [{:keys [map-id k]} op
          ser-k (au/<? (storage/<value->serialized-value
                        storage l/string-schema k))
          arg (assoc op
                     :crdt-key (make-map-keyset-crdt-key map-id)
                     :serialized-value ser-k
                     :storage storage)]
      (au/<? (<add-to-crdt! arg)))))

(defmethod <apply-op! :del-map-key
  [{:keys [storage op]}]
  (let [arg (assoc op
                   :crdt-key (make-map-keyset-crdt-key (:map-id op))
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

(defn <do-conflict-resolution
  [{:keys [<resolve-conflict
           current-value-infos
           crdt-key
           make-add-id
           storage
           subject-id] :as arg
    :or {<resolve-conflict u/<resolve-conflict-lww
         make-add-id u/compact-random-uuid}}]
  (au/go
    ;; IMPORTANT: We must delete all candidates, (not just the losers)
    ;; then add a new entry for the winner with a new add-id.
    ;; If, instead, we only deleted the losers, simultaneous conflict
    ;; resolutions on different clients would not compose properly. Suppose
    ;; client 1 wants to keep candidate A and so deletes candidate B. At the
    ;; same time, client 2 wants to keep candidate B, so deletes candidate A.
    ;; When all states are merged, both candidates would be deleted and the
    ;; value would be nil. By deleting all candidates and adding a new copy
    ;; of the winner, things work properly. In the scenario above, client 1
    ;; deletes all the candidates and adds new value C. Client 2 also
    ;; deletes all the candidates (multiple deletes commute just fine) and
    ;; adds new value D. When all states are merged, we again have a
    ;; conflict, but it is as expected and can be resolved again, without
    ;; unexpected behavior.
    (let [winner (au/<? (<resolve-conflict arg))]
      (doseq [{:keys [add-id]} current-value-infos]
        (au/<? (<del-from-crdt! (u/sym-map add-id crdt-key storage))))
      (au/<? (<add-to-crdt! {:add-id (make-add-id)
                             :serialized-value (:serialized-value winner)
                             :subject-id subject-id
                             :sys-time-ms (u/current-time-ms)
                             :crdt-key crdt-key
                             :storage storage})))))

(defn <get-single-value-crdt-val
  [{:keys [crdt-key schema storage] :as arg}]
  (au/go
    (loop []
      (let [crdt (au/<? (storage/<get storage crdt-key schemas/crdt-schema))
            {:keys [current-value-infos]} crdt]
        (case (count current-value-infos)
          0
          nil

          1
          (->> (first current-value-infos)
               (:serialized-value)
               (storage/<serialized-value->value storage schema)
               (au/<?))

          (do
            (au/<? (<do-conflict-resolution (assoc arg
                                                   :current-value-infos
                                                   current-value-infos)))
            (recur)))))))

(defmulti <get-crdt-val
  ;; TODO: Check that all necessary arg keys are present, including
  ;; those for conflict resolution. Might be best to have <get-crdt-val
  ;; be a regular fn which does the key checking, then calls
  ;; <get-crdt-val* (the multimethod).
  (fn [{:keys [array-id crdt-id k map-id record-id] :as arg}]
    (cond
      (and map-id k) :map-kv
      map-id :map
      (and record-id k) :record-kv
      record-id :record
      (and array-id k) :array-kv
      array-id :array
      crdt-id :crdt-value
      :else (throw (ex-info "Could not determine target crdt type."
                            (u/sym-map arg))))))

(defmethod <get-crdt-val :map-kv
  [{:keys [map-id k] :as arg}]
  (let [crdt-key (make-map-key-value-crdt-key map-id k)]
    (<get-single-value-crdt-val (assoc arg :crdt-key crdt-key))))

(defmethod <get-crdt-val :record-kv
  [{:keys [record-id k] :as arg}]
  (let [crdt-key (make-record-key-value-crdt-key record-id k)]
    (<get-single-value-crdt-val (assoc arg :crdt-key crdt-key))))

(defmethod <get-crdt-val :map
  [{:keys [map-id schema storage] :as arg}]
  (au/go
    (let [keyset-k (make-map-keyset-crdt-key map-id)
          infos (some-> (storage/<get storage keyset-k schemas/crdt-schema)
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
                            arg* (assoc arg
                                        :k k
                                        :schema values-schema)
                            v (au/<? (<get-crdt-val arg*))]
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
  [{:keys [record-id schema storage] :as arg}]
  (au/go
    (let [field-ks (->> (l/edn schema)
                        (:fields)
                        (map :name))
          <get-kv (fn [k]
                    (au/go
                      (let [arg* (assoc arg
                                        :k k
                                        :schema (l/schema-at-path schema [k]))
                            v (au/<? (<get-crdt-val arg*))]
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
