(ns com.oncurrent.zeno.state-providers.crdt.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti process-cmd* (fn [{:keys [cmd schema]}]
                         (let [{:zeno/keys [op]} cmd]
                           (if (c/insert-crdt-cmd-types op)
                             [:zeno/insert* (c/schema->dispatch-type schema)]
                             op))))

(defmulti get-delete-ops (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmulti get-add-ops (fn [{:keys [schema]}]
                        (c/schema->dispatch-type schema)))

(defmethod get-delete-ops :single-value
  [{:keys [crdt growing-path] :as arg}]
  (reduce (fn [acc add-id]
            (conj acc {:add-id add-id
                       :op-type :delete-value
                       :op-path growing-path}))
          #{}
          (keys (:add-id-to-value-info crdt))))

(defmethod get-add-ops :single-value
  [{:keys [cmd-arg make-id growing-path sys-time-ms schema] :as arg}]
  #{{:add-id (make-id)
     :op-type :add-value
     :op-path growing-path
     :serialized-value (l/serialize schema cmd-arg)
     :sys-time-ms sys-time-ms
     :value cmd-arg}})

(defn get-delete-container-ops [{:keys [crdt growing-path]}]
  (reduce (fn [acc add-id]
            (conj acc {:add-id add-id
                       :op-type :delete-container
                       :op-path growing-path}))
          #{}
          (:container-add-ids crdt)))

(defn get-add-container-ops
  [{:keys [crdt growing-path make-id sys-time-ms]}]
  (let [{:keys [container-add-ids]} crdt]
    (if (seq container-add-ids)
      #{}
      #{{:add-id (make-id)
         :op-path growing-path
         :op-type :add-container
         :sys-time-ms sys-time-ms}})))

(defn associative-get-delete-ops
  [{:keys [cmd-path cmd-type crdt growing-path shrinking-path schema]
    :as arg}]
  (let [record? (= :record (l/schema-type schema))
        {:keys [children]} crdt]
    (if (empty? shrinking-path)
      (let [child-ks (if record?
                       (map :name (:fields (l/edn schema)))
                       (keys children))]
        (reduce
         (fn [acc k]
           (let [child-schema (if record?
                                (l/child-schema schema k)
                                (l/child-schema schema))]
             (set/union acc (get-delete-ops
                             (assoc arg
                                    :crdt (get children k)
                                    :growing-path (conj growing-path k)
                                    :schema child-schema)))))
         (get-delete-container-ops arg)
         child-ks))
      (let [[k & ks] shrinking-path
            child-schema (if record?
                           (l/child-schema schema k)
                           (l/child-schema schema))]
        (get-delete-ops (assoc arg
                               :crdt (get children k)
                               :growing-path (conj growing-path k)
                               :schema child-schema
                               :shrinking-path ks))))))

(defmethod get-delete-ops :map
  [arg]
  (associative-get-delete-ops arg))

(defmethod get-delete-ops :record
  [arg]
  (associative-get-delete-ops arg))

(defn associative-get-add-ops
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt growing-path
           schema shrinking-path]
    :as arg}]
  (let [container-ops (get-add-container-ops arg)
        record? (= :record (l/schema-type schema))]
    (if (empty? shrinking-path)
      (let [child-ks (if record?
                       (map :name (:fields (l/edn schema)))
                       (keys cmd-arg))]
        (when (and (= :zeno/set cmd-type)
                   (not (map? cmd-arg)))
          (throw (ex-info
                  (str "The given `:zeno/path` (`" cmd-path "`) indicates a "
                       (name (l/schema-type schema))
                       ", but the given `zeno/arg` (`" (or cmd-arg "nil") "`) "
                       "is not a map.")
                  (u/sym-map cmd-path cmd-arg))))
        (reduce
         (fn [acc k]
           (let [child-schema (if record?
                                (l/child-schema schema k)
                                (l/child-schema schema))
                 v (get cmd-arg k)]
             (set/union acc (get-add-ops
                             (assoc arg
                                    :cmd-arg v
                                    :crdt (get-in crdt [:children k])
                                    :growing-path (conj growing-path k)
                                    :schema child-schema)))))
         container-ops
         child-ks))
      (let [[k & ks] shrinking-path
            child-schema (if record?
                           (l/child-schema schema k)
                           (l/child-schema schema))
            add-ops (get-add-ops (assoc arg
                                        :crdt (get-in crdt
                                                      [:children k])
                                        :growing-path (conj growing-path k)
                                        :schema child-schema
                                        :shrinking-path ks))]
        (set/union container-ops add-ops)))))

(defmethod get-add-ops :map
  [arg]
  (associative-get-add-ops arg))

(defmethod get-add-ops :record
  [arg]
  (associative-get-add-ops arg))

(defmethod get-add-ops :union
  [{:keys [cmd-arg crdt growing-path schema shrinking-path] :as arg}]
  (let [info (if (seq shrinking-path)
               (c/get-union-branch-and-schema-for-key
                {:schema schema
                 :k (first shrinking-path)})
               (c/get-union-branch-and-schema-for-value
                {:schema schema
                 :v cmd-arg}))
        {:keys [union-branch member-schema]} info
        branch-k (keyword (str "branch-" union-branch))]
    (get-add-ops (assoc arg
                        :crdt (get crdt branch-k)
                        :growing-path (conj growing-path union-branch)
                        :schema member-schema))))

(defmethod get-delete-ops :union
  [{:keys [cmd-arg crdt growing-path schema shrinking-path] :as arg}]
  (let [member-schemas (l/member-schemas schema)]
    (reduce (fn [acc union-branch]
              (let [branch-k (keyword (str "branch-" union-branch))
                    member-schema (nth member-schemas union-branch)
                    ops (get-delete-ops
                         (assoc arg
                                :crdt (get crdt branch-k)
                                :growing-path (conj growing-path union-branch)
                                :schema member-schema))]
                (set/union acc ops)))
            #{}
            (range (count member-schemas)))))

(defmethod process-cmd* :zeno/set
  [{:keys [data-schema crdt] :as arg}]
  (let [crdt-ops (set/union (get-delete-ops arg)
                            (get-add-ops arg))
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defmethod process-cmd* :zeno/remove
  [arg]
  (let [crdt-ops (get-delete-ops arg)
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defn associative-do-insert
  [{:keys [cmd cmd-arg cmd-type growing-path schema shrinking-path] :as arg}]
  (when (empty? shrinking-path)
    (let [schema-type (l/schema-type schema)]
      (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                           "an array. Path points to schema type `"
                           schema-type "`.")
                      (u/sym-map cmd growing-path shrinking-path)))))
  (let [[k & ks] shrinking-path
        _ (c/check-key (assoc arg :key k))
        container-ops (get-add-container-ops arg)
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops container-ops))
        record? (= :record (l/schema-type schema))
        child-schema (if record?
                       (l/child-schema schema k)
                       (l/child-schema schema))
        child-crdt (get-in crdt [:children k])
        ret (process-cmd* (assoc arg
                                 :crdt child-crdt
                                 :growing-path (conj growing-path k)
                                 :schema child-schema
                                 :shrinking-path ks))]
    {:crdt (assoc-in crdt [:children k] (:crdt ret))
     :crdt-ops (set/union container-ops (:crdt-ops ret))}))

(defmethod process-cmd* [:zeno/insert* :map]
  [arg]
  (associative-do-insert arg))

(defmethod process-cmd* [:zeno/insert* :record]
  [arg]
  (associative-do-insert arg))

(defmethod process-cmd* [:zeno/insert* :single-value]
  [{:keys [cmd cmd-type schema] :as arg}]
  (let [schema-type (l/schema-type schema)]
    (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                         "an array. Path points to schema type `"
                         schema-type "`.")
                    cmd))))

(defn get-parent-node-id [{:keys [cmd-type crdt norm-i]}]
  (if (c/after-cmd-types cmd-type)
    (get-in crdt [:ordered-node-ids norm-i])
    (if (zero? norm-i)
      c/array-head-node-id
      (get-in crdt [:ordered-node-ids (dec norm-i)]))))

(defn do-single-insert
  [{:keys [cmd-type crdt growing-path make-id norm-i schema] :as arg}]
  (let [node-id (make-id)
        child-ops (get-add-ops
                   (assoc arg
                          :crdt nil
                          :growing-path (conj growing-path node-id)
                          :schema (l/child-schema schema)
                          :shrinking-path []))
        parent-node-id (get-parent-node-id arg)
        peer-node-ids (->> (get-in crdt [:node-id-to-child-nodes
                                         parent-node-id])
                           (map :node-id)
                           (set))
        node-info (u/sym-map node-id parent-node-id peer-node-ids)
        sval (l/serialize shared/crdt-array-node-info-schema node-info)
        node-op {:op-type :add-array-node
                 :op-path growing-path
                 :serialized-value sval
                 :value node-info}
        ops (conj child-ops node-op)]
    {:crdt (apply-ops/apply-ops (assoc arg :crdt-ops ops))
     :crdt-ops ops}))

(defn do-range-insert
  [{:keys [cmd-arg cmd-path cmd-type crdt growing-path make-id norm-i schema]
    :as arg}]
  (when-not (sequential? cmd-arg)
    (throw (ex-info (str "The `:zeno/arg` in a range insert command must be "
                         "sequential. Got `" cmd-arg "`.")
                    (u/sym-map cmd-arg cmd-type cmd-path))))
  (let [range-parent-node-id (get-parent-node-id arg)
        vs (vec cmd-arg)
        node-ids (mapv (constantly make-id) vs)
        ops (reduce
             (fn [acc i]
               (let [node-id (nth node-ids i)
                     new-growing-path (conj growing-path node-id)
                     child-ops (get-add-ops
                                (assoc arg
                                       :cmd-arg (nth vs i)
                                       :crdt nil
                                       :growing-path new-growing-path
                                       :schema (l/child-schema schema)
                                       :shrinking-path []))
                     parent-node-id (if (pos? i)
                                      (nth node-ids (dec i))
                                      range-parent-node-id)
                     peer-node-ids (if (pos? i)
                                     #{}
                                     (->> (get-in crdt [:node-id-to-child-nodes
                                                        parent-node-id])
                                          (map :node-id)
                                          (set)))
                     node-info (u/sym-map node-id parent-node-id peer-node-ids)
                     sval (l/serialize shared/crdt-array-node-info-schema
                                       node-info)
                     node-op {:op-type :add-array-node
                              :op-path growing-path
                              :serialized-value sval
                              :value node-info}]
                 (set/union acc (conj child-ops node-op))))
             #{}
             (range (count cmd-arg)))]
    {:crdt (apply-ops/apply-ops (assoc arg :crdt-ops ops))
     :crdt-ops ops}))

(defmethod process-cmd* [:zeno/insert* :array]
  [{:keys [cmd-type cmd-path crdt growing-path schema shrinking-path] :as arg}]
  (let [container-ops (get-add-container-ops arg)
        {:keys [ordered-node-ids]} crdt
        [i & sub-path] shrinking-path
        _ (when-not (int? i)
            (throw (ex-info (str "Array index must be an integer. Got: `"
                                 (or i "nil") "`.")
                            (u/sym-map cmd-type cmd-path growing-path
                                       i shrinking-path sub-path))))
        array-len (count ordered-node-ids)
        norm-i (u/get-normalized-array-index (u/sym-map array-len i))]
    (when-not norm-i
      (throw (ex-info (str "Array index `" i "` is out of bounds for the "
                           "indicated array, whose length is "
                           array-len".")
                      (u/sym-map array-len i))))
    (if (seq sub-path)
      (let [k (get-in crdt [:ordered-node-ids norm-i])
            ret (process-cmd*
                 (assoc arg
                        :crdt (get-in crdt [:children k])
                        :growing-path (conj growing-path norm-i)
                        :schema (l/child-schema schema)
                        :shrinking-path sub-path))
            new-crdt (assoc-in crdt [:children k] (:crdt ret))]
        {:crdt (:crdt ret)
         :crdt-ops (set/union container-ops (:crdt-ops ret))})
      (let [arg* (assoc arg :crdt crdt :norm-i norm-i)
            ret (if (c/range-cmd-types cmd-type)
                  (do-range-insert arg*)
                  (do-single-insert arg*))]
        {:crdt (:crdt ret)
         :crdt-ops (set/union container-ops (:crdt-ops ret))}))))

(defn process-cmd [{:keys [cmd crdt data-schema root] :as arg}]
  (let [cmd-path (:zeno/path cmd)]
    (when-not (= root (first cmd-path))
      (throw (ex-info (str "Command path root `" (first cmd-path)
                           "` does not match given root `" root "`.")
                      (u/sym-map root cmd-path))))
    (process-cmd* (assoc arg
                         :cmd-arg (:zeno/arg cmd)
                         :cmd-path cmd-path
                         :cmd-type (:zeno/op cmd)
                         :growing-path [root]
                         :schema data-schema
                         :shrinking-path (rest cmd-path)))))

(defn process-cmds [{:keys [cmds crdt data-schema make-id root] :as arg}]
  (when-not (keyword? root)
    (throw (ex-info (str "Bad `:root` arg in call to `process-cmds`. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map cmds root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to `process-cmds`. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map cmds data-schema))))
  (let [make-id* (or make-id u/compact-random-uuid)
        sys-time-ms (or (:sys-time-ms arg) (u/current-time-ms))]
    (reduce (fn [acc cmd]
              (let [ret (process-cmd (assoc arg
                                            :crdt (:crdt acc)
                                            :cmd cmd
                                            :make-id make-id*
                                            :sys-time-ms sys-time-ms))]
                (-> acc
                    (assoc :crdt (:crdt ret))
                    (update :crdt-ops set/union (:crdt-ops ret)))))
            {:crdt crdt
             :crdt-ops #{}}
            cmds)))
