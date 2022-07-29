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
                           (if (c/insert-cmd-types op)
                             [:zeno/insert* (c/schema->dispatch-type schema)]
                             op))))

(defmulti get-delete-info (fn [{:keys [schema]}]
                            (c/schema->dispatch-type schema)))

(defmulti get-add-info (fn [{:keys [schema]}]
                         (c/schema->dispatch-type schema)))

(declare do-range-insert)

(defmethod get-delete-info :single-value
  [{:keys [crdt growing-path schema] :as arg}]
  (reduce (fn [acc add-id]
            (let [op {:add-id add-id
                      :op-type :delete-value
                      :op-path growing-path}]
              (-> acc
                  (assoc :crdt (apply-ops/apply-op (assoc op
                                                          :crdt (:crdt acc)
                                                          :schema schema)))
                  (update :crdt-ops conj op))))
          {:crdt crdt
           :crdt-ops #{}}
          (keys (:add-id-to-value-info crdt))))

(defmethod get-add-info :single-value
  [{:keys [cmd-arg crdt make-id growing-path sys-time-ms schema] :as arg}]
  (let [op {:add-id (make-id)
            :op-type :add-value
            :op-path growing-path
            :serialized-value {:bytes (l/serialize schema cmd-arg)
                               :fp (l/fingerprint128 schema)}
            :sys-time-ms sys-time-ms
            :value cmd-arg}
        crdt-ops #{op}
        crdt (apply-ops/apply-op (assoc op
                                        :crdt crdt
                                        :schema schema))]
    (u/sym-map crdt crdt-ops)))

(defn get-delete-container-info [{:keys [crdt growing-path schema]}]
  (reduce (fn [acc add-id]
            (let [op {:add-id add-id
                      :op-type :delete-container
                      :op-path growing-path}]
              (-> acc
                  (assoc :crdt (apply-ops/apply-op (assoc op
                                                          :crdt (:crdt acc)
                                                          :schema schema)))
                  (update :crdt-ops conj op))))
          {:crdt crdt
           :crdt-ops #{}}
          (:container-add-ids crdt)))

(defn get-add-container-info
  [{:keys [crdt growing-path make-id schema sys-time-ms]}]
  (let [{:keys [container-add-ids]} crdt]
    (if (seq container-add-ids)
      {:crdt crdt
       :crdt-ops #{}}
      (let [op {:add-id (make-id)
                :op-path growing-path
                :op-type :add-container
                :sys-time-ms sys-time-ms}
            crdt-ops #{op}
            crdt (apply-ops/apply-op (assoc op
                                            :crdt crdt
                                            :schema schema))]
        (u/sym-map crdt crdt-ops)))))

(defn associative-get-delete-info
  [{:keys [crdt growing-path shrinking-path schema]
    :as arg}]
  (let [schema-type (l/schema-type schema)]
    (if (empty? shrinking-path)
      (let [container-info (get-delete-container-info arg)
            child-ks (case schema-type
                       :array (-> container-info :crdt :ordered-node-ids)
                       :map (-> container-info :crdt :children keys)
                       :record (map :name (:fields (l/edn schema))))]
        (reduce
         (fn [acc k]
           (let [child-schema (if (= :record schema-type)
                                (l/child-schema schema k)
                                (l/child-schema schema))
                 child-info (get-delete-info
                             (assoc arg
                                    :crdt (get-in acc [:crdt :children k])
                                    :growing-path (conj growing-path k)
                                    :schema child-schema))]
             (-> acc
                 (assoc-in [:crdt :children k] (:crdt child-info))
                 (update :crdt-ops set/union (:crdt-ops child-info)))))
         container-info
         child-ks))
      (let [[i-or-k & ks] shrinking-path
            k (if (= :array schema-type)
                (-> crdt :ordered-node-ids (nth i-or-k))
                i-or-k)
            child-schema (if (= :record schema-type)
                           (l/child-schema schema k)
                           (l/child-schema schema))
            child-info (get-delete-info
                        (assoc arg
                               :crdt (get-in crdt [:children k])
                               :growing-path (conj growing-path k)
                               :schema child-schema
                               :shrinking-path ks))]
        {:crdt (assoc-in crdt [:children k] (:crdt child-info))
         :crdt-ops (:crdt-ops child-info)}))))

(defmethod get-delete-info :array
  [arg]
  (associative-get-delete-info arg))

(defmethod get-delete-info :map
  [arg]
  (associative-get-delete-info arg))

(defmethod get-delete-info :record
  [arg]
  (associative-get-delete-info arg))

(defn associative-get-add-info
  [{:keys [cmd-arg cmd-path cmd-type growing-path schema shrinking-path]
    :as arg}]
  (let [container-info (get-add-container-info arg)
        schema-type (l/schema-type schema)]
    (if (empty? shrinking-path)
      (if (= :array schema-type)
        (let [info (do-range-insert (assoc arg
                                           :crdt (:crdt container-info)
                                           :norm-i 0))]
          (update info :crdt-ops set/union (:crdt-ops container-info)))
        (let [_ (when (and (= :zeno/set cmd-type)
                           (not (map? cmd-arg)))
                  (throw (ex-info
                          (str "The given `:zeno/path` (`" cmd-path
                               "`) indicates a " (name (l/schema-type schema))
                               ", but the given `zeno/arg` (`"
                               (or cmd-arg "nil") "`) is not a map.")
                          (u/sym-map cmd-path cmd-arg))))
              child-ks (case schema-type
                         :map (keys cmd-arg)
                         :record (->> (:fields (l/edn schema))
                                      (map :name)
                                      (filter (into #{} (keys cmd-arg)))))]
          (reduce
           (fn [acc k]
             (let [child-schema (if (= :record schema-type)
                                  (l/child-schema schema k)
                                  (l/child-schema schema))
                   v (get cmd-arg k)
                   child-info (get-add-info
                               (assoc arg
                                      :cmd-arg v
                                      :crdt (get-in acc [:crdt :children k])
                                      :growing-path (conj growing-path k)
                                      :schema child-schema))]
               (-> acc
                   (assoc-in [:crdt :children k] (:crdt child-info))
                   (update :crdt-ops set/union (:crdt-ops child-info)))))
           container-info
           child-ks)))
      (let [[i-or-k & ks] shrinking-path
            k (if (= :array schema-type)
                (-> container-info :crdt :ordered-node-ids (nth i-or-k))
                i-or-k)
            child-crdt (get-in container-info [:crdt :children k])
            child-schema (if (= :record schema-type)
                           (l/child-schema schema k)
                           (l/child-schema schema))
            child-info (get-add-info (assoc arg
                                            :crdt child-crdt
                                            :growing-path (conj growing-path k)
                                            :schema child-schema
                                            :shrinking-path ks))]
        (-> container-info
            (assoc-in [:crdt :children k] (:crdt child-info))
            (update :crdt-ops set/union (:crdt-ops child-info)))))))

(defmethod get-add-info :array
  [arg]
  (associative-get-add-info arg))

(defmethod get-add-info :map
  [arg]
  (associative-get-add-info arg))

(defmethod get-add-info :record
  [arg]
  (associative-get-add-info arg))

(defmethod get-add-info :union
  [{:keys [cmd-arg crdt growing-path schema shrinking-path sys-time-ms]
    :as arg}]
  (let [branch-info (if (seq shrinking-path)
                      (c/get-union-branch-and-schema-for-key
                       {:schema schema
                        :k (first shrinking-path)})
                      (c/get-union-branch-and-schema-for-value
                       {:schema schema
                        :v cmd-arg}))
        {:keys [union-branch member-schema]} branch-info
        branch-k (keyword (str "branch-" union-branch))
        branch-time-k (keyword (str "branch-"  union-branch "-sys-time-ms"))]
    (-> (get-add-info (assoc arg
                             :crdt (get crdt branch-k)
                             :growing-path (conj growing-path union-branch)
                             :schema member-schema))

        (update :crdt (fn [crdt*] {branch-time-k sys-time-ms
                                   branch-k crdt*})))))

(defmethod get-delete-info :union
  [{:keys [cmd-arg crdt growing-path schema shrinking-path] :as arg}]
  (let [member-schemas (l/member-schemas schema)]
    (reduce (fn [acc union-branch]
              (let [branch-k (keyword (str "branch-" union-branch))
                    member-schema (nth member-schemas union-branch)
                    info (get-delete-info
                          (assoc arg
                                 :crdt (get-in acc [:crdt branch-k])
                                 :growing-path (conj growing-path union-branch)
                                 :schema member-schema))]
                (-> acc
                    (assoc :crdt (:crdt info))
                    (update :crdt-ops set/union (:crdt-ops info)))))
            {:crdt crdt
             :crdt-ops #{}}
            (range (count member-schemas)))))

(defmethod process-cmd* :zeno/set
  [{:keys [crdt] :as arg}]
  (let [delete-info (get-delete-info arg)
        add-info (get-add-info (assoc arg :crdt (:crdt delete-info)))]
    {:crdt (:crdt add-info)
     :crdt-ops (set/union (:crdt-ops add-info) (:crdt-ops delete-info))}))

(defmethod process-cmd* :zeno/remove
  [arg]
  (get-delete-info arg))

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
        info (get-add-container-info arg)
        is-record? (= :record (l/schema-type schema))
        child-schema (if is-record?
                       (l/child-schema schema k)
                       (l/child-schema schema))
        ret (process-cmd* (assoc arg
                                 :crdt (get-in info [:crdt :children k])
                                 :growing-path (conj growing-path k)
                                 :schema child-schema
                                 :shrinking-path ks))]
    (-> info
        (assoc-in [:crdt :children k] (:crdt ret))
        (update :crdt-ops set/union (:crdt-ops ret)))))

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
  (let [before? (c/insert-before-cmd-types cmd-type)]
    (if (or (empty? (:ordered-node-ids crdt))
            (and (zero? norm-i)
                 before?))
      c/array-head-node-id
      (get-in crdt [:ordered-node-ids (if before?
                                        (dec norm-i)
                                        norm-i)]))))

(defn do-single-insert
  [{:keys [cmd-type growing-path make-id norm-i schema] :as arg}]
  (when-not (int? norm-i)
    (throw (ex-info (str "Bad `:norm-i` in call to `do-single-insert`. Got `"
                         (or norm-i "nil") "`.")
                    (u/sym-map norm-i))))
  (let [node-id (make-id)
        child-info (get-add-info
                    (assoc arg
                           :crdt nil
                           :growing-path (conj growing-path node-id)
                           :schema (l/child-schema schema)
                           :shrinking-path []))
        crdt (assoc-in (:crdt arg) [:children node-id] (:crdt child-info))
        parent-node-id (get-parent-node-id arg)
        peer-node-ids (->> (get-in crdt [:node-id-to-child-infos
                                         parent-node-id])
                           (map :node-id)
                           (set))
        node-info (u/sym-map node-id parent-node-id peer-node-ids)
        sval {:bytes (l/serialize shared/crdt-array-node-info-schema node-info)
              :fp (l/fingerprint128 shared/crdt-array-node-info-schema)}
        node-op {:op-type :add-array-node
                 :op-path growing-path
                 :serialized-value sval
                 :value node-info}
        node-crdt (apply-ops/apply-op (assoc node-op
                                             :crdt crdt
                                             :schema schema))]
    {:crdt node-crdt
     :crdt-ops (conj (:crdt-ops child-info) node-op)}))

(defn do-range-insert
  [{:keys [cmd-arg cmd-path cmd-type growing-path make-id norm-i schema]
    :as arg}]
  (when-not (sequential? cmd-arg)
    (throw (ex-info (str "The `:zeno/arg` in a range insert command must be "
                         "sequential. Got `" cmd-arg "`.")
                    (u/sym-map cmd-arg cmd-type cmd-path))))
  (when-not (int? norm-i)
    (throw (ex-info (str "Bad `:norm-i` in call to `do-range-insert`. Got `"
                         (or norm-i "nil") "`.")
                    (u/sym-map norm-i))))
  (let [range-parent-node-id (get-parent-node-id arg)
        vs (vec cmd-arg)
        node-ids (mapv (fn [v]
                         (make-id))
                       vs)]
    (reduce
     (fn [acc i]
       (let [node-id (nth node-ids i)
             new-growing-path (conj growing-path node-id)
             child-info (get-add-info
                         (assoc arg
                                :crdt nil
                                :cmd-arg (nth vs i)
                                :growing-path new-growing-path
                                :schema (l/child-schema schema)
                                :shrinking-path []))
             crdt (assoc-in (:crdt acc) [:children node-id] (:crdt child-info))
             parent-node-id (if (pos? i)
                              (nth node-ids (dec i))
                              range-parent-node-id)
             peer-node-ids (if (pos? i)
                             #{}
                             (->> (get-in crdt [:node-id-to-child-infos
                                                parent-node-id])
                                  (map :node-id)
                                  (set)))
             node-info (u/sym-map node-id parent-node-id peer-node-ids)
             sval {:bytes (l/serialize
                           shared/crdt-array-node-info-schema
                           node-info)
                   :fp (l/fingerprint128
                        shared/crdt-array-node-info-schema)}
             node-op {:op-type :add-array-node
                      :op-path growing-path
                      :serialized-value sval
                      :value node-info}
             node-crdt (apply-ops/apply-op (assoc node-op
                                                  :crdt crdt
                                                  :schema schema))]
         (-> acc
             (assoc :crdt node-crdt)
             (update :crdt-ops #(set/union % (conj (:crdt-ops child-info)
                                                   node-op))))))
     {:crdt (:crdt arg)
      :crdt-ops #{}}
     (range (count cmd-arg)))))

(defmethod process-cmd* [:zeno/insert* :array]
  [{:keys [cmd-type cmd-path growing-path schema shrinking-path] :as arg}]
  (let [{:keys [crdt crdt-ops]} (get-add-container-info arg)
        [i & sub-path] shrinking-path
        _ (when-not (int? i)
            (throw (ex-info (str "Array index must be an integer. Got: `"
                                 (or i "nil") "`.")
                            (u/sym-map cmd-type cmd-path growing-path
                                       i shrinking-path sub-path))))
        array-len (count (:ordered-node-ids crdt))
        norm-i (or (u/get-normalized-array-index (u/sym-map array-len i))
                   ;; On empty arrays,
                   ;;   - Allow insert before 0
                   ;;   - Allow insert after -1
                   (when (and (zero? array-len)
                              (or (and (c/insert-before-cmd-types cmd-type)
                                       (zero? i))
                                  (and (c/insert-after-cmd-types cmd-type)
                                       (= -1 i))))
                     0))]
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
                        :shrinking-path sub-path))]
        {:crdt (assoc-in crdt [:children k] (:crdt ret))
         :crdt-ops (set/union crdt-ops (:crdt-ops ret))})
      (let [arg* (assoc arg :crdt crdt :norm-i norm-i)
            ret (if (c/range-cmd-types cmd-type)
                  (do-range-insert arg*)
                  (do-single-insert arg*))]
        {:crdt (:crdt ret)
         :crdt-ops (set/union crdt-ops (:crdt-ops ret))}))))

(defn process-cmd [{:keys [cmd crdt data-schema root] :as arg}]
  (when-not (keyword? root)
    (throw (ex-info (str "Bad `:root` arg in call to `process-cmd`. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map cmd root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to `process-cmd`. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map cmd data-schema))))
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
