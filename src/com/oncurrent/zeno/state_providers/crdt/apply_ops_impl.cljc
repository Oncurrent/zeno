(ns com.oncurrent.zeno.state-providers.crdt.apply-ops-impl
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def crdt-value-info-keys [:sys-time-ms :value])

(defmulti apply-op (fn [{:keys [op-type schema]}]
                     [(c/schema->dispatch-type schema) op-type]))

(defn same-cv-info? [x y]
  (and x
       y
       (= (select-keys x crdt-value-info-keys)
          (select-keys y crdt-value-info-keys))))

(defn add-single-value
  [{:keys [add-id crdt op-type] :as arg}]
  (let [{:keys [current-add-id-to-value-info deleted-add-ids]} crdt
        cur-value-info (get current-add-id-to-value-info add-id)
        value-info (select-keys arg crdt-value-info-keys)
        same? (same-cv-info? cur-value-info value-info)
        deleted? (get deleted-add-ids add-id)]
    (when-not (:sys-time-ms arg)
      (throw (ex-info "`:add-value` op is missing `:sys-time-ms`."
                      (u/sym-map op-type add-id value-info))))
    (when (and value-info cur-value-info (not same?))
      (throw (ex-info
              (str "Attempt to reuse an existing add-id "
                   "(`" add-id "`) to add different value info to CRDT.")
              (u/sym-map add-id cur-value-info op-type value-info))))
    (if (or deleted? same?)
      crdt
      (assoc-in crdt [:current-add-id-to-value-info add-id] value-info))))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt op-path] :as arg}]
  (if (seq op-path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map add-id crdt op-path)))
    (add-single-value arg)))

(defn del-single-value
  [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-id-to-value-info dissoc add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defmethod apply-op [:single-value :delete-value]
  [arg]
  (del-single-value arg))

(defn associative-apply-op
  [{:keys [get-child-schema crdt op-path]
    :as arg}]
  (let [[k & ks] op-path]
    (c/check-key (assoc arg :key k :string-array-keys? true))
    (update-in crdt [:children k]
               (fn [child-crdt]
                 (apply-op (assoc arg
                                  :crdt child-crdt
                                  :op-path ks
                                  :schema (get-child-schema k)))))))

(defn check-path-union-branch
  [{:keys [add-id op-type op-path schema value] :as arg}]
  (let [[union-branch & ks] op-path]
    (when-not (int? union-branch)
      (let [union-edn-schema (l/edn schema)]
        (throw (ex-info
                (str "Operating on ao op-path that includes a union requires "
                     "an integer union-branch value in the op-path. Got `"
                     (or union-branch "nil") "`.")
                (u/sym-map add-id op-type op-path union-edn-schema value)))))))

(defn apply-union-op [{:keys [op-path schema write-branch?] :as arg}]
  (check-path-union-branch arg)
  (let [[union-branch & ks] op-path
        member-schema (l/member-schema-at-branch schema union-branch)
        arg* (cond-> (assoc arg
                            :op-path ks
                            :schema member-schema)
               true (dissoc :write-branch?)
               write-branch? (assoc-in [:crdt :union-branch] union-branch))]
    (apply-op arg*)))

(defmethod apply-op [:union :add-value]
  [arg]
  (apply-union-op (assoc arg :write-branch? true)))

(defmethod apply-op [:union :delete-value]
  [arg]
  (apply-union-op arg))

(defmethod apply-op [:union :add-array-edge]
  [{:keys [op-path] :as arg}]
  (apply-union-op (assoc-in arg [:crdt :union-branch] (first op-path))))

(defmethod apply-op [:union :delete-array-edge]
  [arg]
  (apply-union-op arg))

(defmethod apply-op [:union :add-container]
  [arg]
  (apply-union-op (assoc arg :write-branch? true)))

(defmethod apply-op [:union :delete-container]
  [arg]
  (apply-union-op arg))

(defn delete-container [{:keys [crdt get-child-schema op-path] :as arg}]
  (if (seq op-path)
    (let [[k & ks] op-path]
      (c/check-key (assoc arg :key k :string-array-keys? true))
      (update-in crdt [:children k]
                 (fn [child-crdt]
                   (apply-op (assoc arg
                                    :crdt child-crdt
                                    :op-path ks
                                    :schema (get-child-schema k))))))
    (let [{add-ids-to-delete :value} arg]
      (-> crdt
          (update :container-add-ids set/difference add-ids-to-delete)
          (update :deleted-container-add-ids
                  (fn [add-ids]
                    (if (seq add-ids)
                      (set/union add-ids add-ids-to-delete)
                      add-ids-to-delete)))))))

(defn add-container [{:keys [add-id crdt get-child-schema op-path] :as arg}]
  (if (seq op-path)
    (let [[k & ks] op-path]
      (c/check-key (assoc arg :key k :string-array-keys? true))
      (update-in crdt [:children k]
                 (fn [child-crdt]
                   (apply-op (assoc arg
                                    :crdt child-crdt
                                    :op-path ks
                                    :schema (get-child-schema k))))))
    (let [{:keys [deleted-container-add-ids]} crdt]
      (if (contains? deleted-container-add-ids add-id)
        crdt
        (update crdt :container-add-ids (fn [add-ids]
                                          (if (seq add-ids)
                                            (conj add-ids add-id)
                                            #{add-id})))))))

(defmethod apply-op [:map :add-value]
  [{:keys [add-id op-path schema value] :as arg}]
  (when (empty? op-path)
    (throw (ex-info (str "Path indicates a map, but no map key is given "
                         "in the op-path.")
                    (u/sym-map add-id op-path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               (fn [_] (l/child-schema schema)))))

(defmethod apply-op [:map :add-container]
  [{:keys [schema] :as arg}]
  (add-container (assoc arg :get-child-schema
                        (constantly (l/child-schema schema)))))

(defn associative-delete-value
  [{:keys [add-id get-child-schema crdt op-path] :as arg}]
  (associative-apply-op arg))

(defmethod apply-op [:map :delete-value]
  [{:keys [schema] :as arg}]
  (associative-delete-value (assoc arg :get-child-schema
                                   (fn [_] (l/child-schema schema)))))

(defmethod apply-op [:map :delete-container]
  [{:keys [schema] :as arg}]
  (delete-container (assoc arg :get-child-schema
                           (constantly (l/child-schema schema)))))

(defmethod apply-op [:map :add-array-edge]
  [{:keys [add-id crdt op-path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               (fn [_] (l/child-schema schema)))))

(defmethod apply-op [:map :delete-array-edge]
  [{:keys [add-id crdt op-path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               (fn [_] (l/child-schema schema)))))

(defmethod apply-op [:record :add-value]
  [{:keys [add-id op-path schema value] :as arg}]
  (when (empty? op-path)
    (throw (ex-info (str "Schema indicates a record, but no record key is "
                         "given in the op-path.")
                    (u/sym-map add-id op-path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/child-schema schema %))))

(defmethod apply-op [:record :add-container]
  [{:keys [schema] :as arg}]
  (add-container
   (assoc arg :get-child-schema #(l/child-schema schema %))))

(defmethod apply-op [:record :delete-value]
  [{:keys [schema] :as arg}]
  (associative-delete-value (assoc arg :get-child-schema
                                   #(l/child-schema schema %))))

(defmethod apply-op [:record :delete-container]
  [{:keys [schema] :as arg}]
  (delete-container (assoc arg :get-child-schema #(l/child-schema schema %))))

(defmethod apply-op [:record :add-array-edge]
  [{:keys [add-id crdt op-path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/child-schema schema %))))

(defmethod apply-op [:record :delete-array-edge]
  [{:keys [add-id crdt op-path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/child-schema schema %))))

(defmethod apply-op [:array :add-value]
  [{:keys [add-id op-path schema value] :as arg}]
  (when (empty? op-path)
    (throw (ex-info (str "Schema indicates an array, but no array key is given "
                         "in the op-path.")
                    (u/sym-map add-id op-path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               (fn [_] (l/child-schema schema)))))

(defmethod apply-op [:array :add-container]
  [{:keys [schema] :as arg}]
  (add-container
   (assoc arg :get-child-schema (constantly (l/child-schema schema)))))

(defmethod apply-op [:array :delete-value]
  [{:keys [schema] :as arg}]
  (let [get-child-schema (fn [_] (l/child-schema schema))]
    (associative-delete-value (assoc arg :get-child-schema get-child-schema))))

(defn check-edge [{:keys [add-id op-path] :as arg}]
  (let [edge (:value arg)]
    (doseq [k [:head-node-id :tail-node-id]]
      (let [v (get edge k)]
        (when (nil? v)
          (throw (ex-info (str "Missing " k " in edge.")
                          (u/sym-map add-id op-path edge))))
        (when-not (string? v)
          (throw (ex-info (str k " in edge is not a string. Got `" v "`.")
                          (u/sym-map add-id op-path edge))))))))

(defmethod apply-op [:array :add-array-edge]
  [{:keys [add-id crdt op-path schema value] :as arg}]
  (if (seq op-path)
    (associative-apply-op (assoc arg :get-child-schema
                                 (fn [_] (l/child-schema schema))))
    ;; We use a slightly different CRDT implementation here because we
    ;; need to be able to resurrect deleted edges. The `:single-value`
    ;; CRDT does not keep info for deleted items.
    (let [{:keys [add-id-to-edge
                  current-edge-add-ids
                  deleted-edge-add-ids]} crdt
          deleted? (get deleted-edge-add-ids add-id)
          current? (get current-edge-add-ids add-id)
          edge (get add-id-to-edge add-id)
          same? (= edge value)]
      (when (and edge value (not same?))
        (throw (ex-info
                (str "Attempt to reuse an existing add-id "
                     "(`" add-id "`) to add a different edge to CRDT.")
                arg)))
      (check-edge arg)
      (cond-> (assoc-in crdt [:add-id-to-edge add-id] value)
        (not deleted?)
        (update :current-edge-add-ids (fn [ids]
                                        (if (seq ids)
                                          (conj ids add-id)
                                          #{add-id})))))))

(defmethod apply-op [:array :delete-array-edge]
  [{:keys [add-id cmd crdt op-path schema value] :as arg}]
  (if (seq op-path)
    (associative-delete-value (assoc arg :get-child-schema
                                     (fn [_] (l/child-schema schema))))
    ;; We use a slightly different CRDT implementation here because we
    ;; need to be able to resurrect deleted edges. The `:single-value`
    ;; CRDT does not keep info for deleted items.
    (-> crdt
        (update :current-edge-add-ids disj add-id)
        (update :deleted-edge-add-ids (fn [ids]
                                        (if (seq ids)
                                          (conj ids add-id)
                                          #{add-id}))))))

(defmethod apply-op [:array :delete-container]
  [{:keys [schema] :as arg}]
  (delete-container
   (assoc arg :get-child-schema (constantly (l/child-schema schema)))))

(defn apply-ops-without-repair
  [{:keys [crdt crdt-ops schema] :as arg}]
  (reduce
   (fn [acc {:keys [sys-time-ms] :as op}]
     (apply-op (assoc op
                      :crdt acc
                      :schema schema
                      :sys-time-ms (or sys-time-ms
                                       (:sys-time-ms arg)))))
   crdt
   crdt-ops))
