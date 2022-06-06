(ns com.oncurrent.zeno.state-providers.crdt.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.array :as array]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def insert-crdt-ops #{:zeno/insert-after
                  :zeno/insert-before
                  :zeno/insert-range-after
                  :zeno/insert-range-before})

(defmulti process-cmd* (fn [{:keys [cmd]}]
                         (let [{:zeno/keys [op]} cmd]
                           (if (insert-crdt-ops op)
                             :zeno/insert*
                             op))))

(defmulti do-insert (fn [{:keys [schema]}]
                      (c/schema->dispatch-type schema)))

(defmulti do-repair (fn [{:keys [schema]}]
                      (l/schema-type schema)))

(defmulti get-delete-crdt-ops (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmulti get-add-crdt-ops (fn [{:keys [schema]}]
                             (c/schema->dispatch-type schema)))

(defn xf-op-paths [{:keys [prefix crdt-ops]}]
  (reduce (fn [acc op]
            (conj acc (update op :op-path #(cons prefix %))))
          #{}
          crdt-ops))

(defmethod get-delete-crdt-ops :single-value
  [{:keys [crdt]}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (reduce-kv (fn [acc add-id value-info]
                 (conj acc {:add-id add-id
                            :op-path '()
                            :op-type :delete-value}))
               #{}
               current-add-id-to-value-info)))

(defn associative-get-delete-crdt-ops
  [{:keys [crdt get-child-path-info get-child-schema path schema array?]
    :as arg}]
  (let [get-child-crdt-ops (fn [{:keys [k i sub-path]}]
                        (-> (get-delete-crdt-ops
                             (assoc arg
                                    :crdt (get-in crdt [:children k])
                                    :path sub-path
                                    :schema (get-child-schema k)))
                            ((fn [crdt-ops]
                               (xf-op-paths
                                (cond-> {:prefix k
                                         :crdt-ops crdt-ops}
                                  i (assoc :i i :array? true)))))))]
    (if (empty? path)
      (do
       (reduce-kv (fn [acc k _]
                    (let [ret (get-child-crdt-ops (cond-> {:k k
                                                      :sub-path []}
                                               array? (assoc :i 0)))]
                     (set/union acc ret)))
                 #{}
                 (:children crdt)))
      (-> path
          (get-child-path-info)
          (get-child-crdt-ops)))))

(defmethod get-delete-crdt-ops :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (associative-get-delete-crdt-ops
     (assoc arg
            :get-child-path-info (fn [[k & sub-path]]
                                   (u/sym-map k sub-path))
            :get-child-schema (constantly values-schema)))))

(defmethod get-delete-crdt-ops :record
  [{:keys [cmd path schema] :as arg}]
  (associative-get-delete-crdt-ops
   (assoc arg
          :get-child-path-info (fn [[k & sub-path]]
                                 (u/sym-map k sub-path))
          :get-child-schema (fn [k]
                              (or (l/child-schema schema k)
                                  (throw (ex-info (str "Bad record key `" k
                                                       "` in path `"
                                                       path "`.")
                                                  (u/sym-map path k cmd))))))))

(defn get-crdt-ops-del-single-node
  [{:keys [crdt make-id node-id sys-time-ms]}]
  (let [{:keys [add-id-to-edge current-edge-add-ids]} crdt
        info (reduce
              (fn [acc add-id]
                (let [edge (add-id-to-edge add-id)
                      new-acc (cond
                                (= node-id (:tail-node-id edge))
                                (assoc acc :in-add-id add-id)

                                (= node-id (:head-node-id edge))
                                (assoc acc :out-add-id add-id)

                                :else
                                acc)]
                  (if (and (:in-add-id new-acc) (:out-add-id new-acc))
                    (reduced new-acc)
                    new-acc)))
              {:in-add-id nil
               :out-add-id nil}
              current-edge-add-ids)
        {:keys [in-add-id out-add-id]} info]
    #{{:add-id in-add-id
       :op-path '()
       :op-type :delete-array-edge}
      {:add-id out-add-id
       :op-path '()
       :op-type :delete-array-edge}
      {:add-id (make-id)
       :op-path '()
       :op-type :add-array-edge
       :sys-time-ms (or sys-time-ms (u/current-time-ms))
       :value {:head-node-id (:head-node-id (add-id-to-edge in-add-id))
               :tail-node-id (:tail-node-id (add-id-to-edge out-add-id))}}}))

(defmethod get-delete-crdt-ops :array
  [{:keys [make-id cmd crdt path schema sys-time-ms] :as arg}]
  (let [items-schema (l/child-schema schema)
        ordered-node-ids (array/get-ordered-node-ids arg)
        node-crdt-ops (associative-get-delete-crdt-ops
                  (assoc arg
                         :array? true
                         :get-child-path-info
                         (fn [[i & sub-path]]
                           (when-not (integer? i)
                             (throw (ex-info
                                     (str "Index into array must be an "
                                          "integer. Got: `" (or i "nil") "`.")
                                     (u/sym-map i sub-path path cmd))))
                           (let [ni (u/get-normalized-array-index
                                     {:array-len (count ordered-node-ids)
                                      :i i})]
                             (when (or (not ni) (empty? ordered-node-ids))
                               (throw
                                (ex-info
                                 (str "Index `" ni "` into array `"
                                      ordered-node-ids "` is out of bounds.")
                                 (u/sym-map cmd path ordered-node-ids
                                            i sub-path))))
                             {:k (nth ordered-node-ids ni)
                              :i i
                              :sub-path sub-path}))
                         :get-child-schema (constantly items-schema)))
        {:keys [add-id-to-edge current-edge-add-ids]} crdt
        [i & sub-path] path
        edge-crdt-ops (cond
                   (empty? path)
                   (reduce (fn [acc eaid]
                             (conj acc {:add-id eaid
                                        :op-path '()
                                        :op-type :delete-array-edge}))
                           #{}
                           current-edge-add-ids)

                   (and (empty? sub-path)
                        (= :zeno/remove (:zeno/op cmd)))
                   (let [_ (when (or (> i (count ordered-node-ids))
                                     (empty? ordered-node-ids))
                             (throw (ex-info
                                     (str "Index `" i "` into array `"
                                          ordered-node-ids "` is out of bounds.")
                                     (u/sym-map cmd path ordered-node-ids
                                                add-id-to-edge current-edge-add-ids
                                                i sub-path))))
                         node-id (nth ordered-node-ids i)]
                     (get-crdt-ops-del-single-node (u/sym-map crdt make-id node-id
                                                         sys-time-ms)))

                   :else
                   #{})]
    (set/union node-crdt-ops edge-crdt-ops)))

(defmethod get-delete-crdt-ops :union
  [{:keys [crdt path schema] :as arg}]
  (let [{:keys [union-branch]} crdt
        member-schema (when union-branch
                        (l/member-schema-at-branch schema union-branch))]
    (if member-schema
      (-> (get-delete-crdt-ops (assoc arg
                                 :path path
                                 :schema member-schema))
          ((fn [crdt-ops]
             (xf-op-paths {:prefix union-branch
                           :crdt-ops crdt-ops
                           :union? true}))))
      #{})))

(defmethod get-add-crdt-ops :single-value
  [{:keys [cmd-arg make-id sys-time-ms]}]
  #{{:add-id (make-id)
     :op-path '()
     :op-type :add-value
     :sys-time-ms (or sys-time-ms (u/current-time-ms))
     :value cmd-arg}})

(defmethod get-add-crdt-ops :map
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt path schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (if (seq path)
      (let [[k & ks] path]
        (-> (get-add-crdt-ops (assoc arg
                                :crdt (get-in crdt [:children k])
                                :path ks
                                :schema values-schema))
            ((fn [crdt-ops]
               (xf-op-paths {:prefix k
                             :crdt-ops crdt-ops})))))
      (let [edn-schema (l/edn schema)
            pred (c/edn-schema->pred edn-schema)
            _ (when (and (= :zeno/set cmd-type) (not (pred cmd-arg)))
                (throw (ex-info
                        (str ":arg (`" (or cmd-arg "nil") "`) is not the "
                             "correct type for this command.")
                        (u/sym-map cmd path))))]
        (reduce-kv (fn [acc k v]
                     (let [add-crdt-ops (get-add-crdt-ops
                                    (assoc arg
                                           :cmd-arg v
                                           :crdt (get-in crdt [:children k])
                                           :path []
                                           :schema values-schema))]
                       (set/union acc
                                  (xf-op-paths {:prefix k
                                                :crdt-ops add-crdt-ops}))))
                   #{}
                   cmd-arg)))))

(defmethod get-add-crdt-ops :record
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt path schema] :as arg}]
  (if (seq path)
    (let [[k & ks] path]
      (-> (get-add-crdt-ops (assoc arg
                              :crdt (get-in crdt [:children k])
                              :path ks
                              :schema (l/child-schema schema k)))
          ((fn [crdt-ops]
             (xf-op-paths {:prefix k
                           :crdt-ops crdt-ops})))))
    (do
      (when (and (= :zeno/set cmd-type) (not (associative? cmd-arg)))
        (throw (ex-info
                (str "Command path indicates a record, but `:arg` is "
                     "not associative. Got `" (or cmd-arg "nil") "`.")
                (u/sym-map cmd path))))
      (reduce (fn [acc k]
                (let [v (get cmd-arg k)
                      child-schema (l/child-schema schema k)
                      add-crdt-ops (get-add-crdt-ops
                               (assoc arg
                                      :cmd-arg v
                                      :crdt (get-in crdt [:children k])
                                      :path []
                                      :schema child-schema))]
                  (set/union acc
                             (xf-op-paths {:prefix k
                                           :crdt-ops add-crdt-ops}))))
              #{}
              (->> (l/edn schema)
                   (:fields)
                   (map :name))))))

(defmethod get-add-crdt-ops :union
  [{:keys [cmd-arg path schema] :as arg}]
  (let [ret (if (seq path)
              (c/get-union-branch-and-schema-for-key {:schema schema
                                                      :k (first path)})
              (c/get-union-branch-and-schema-for-value {:schema schema
                                                        :v cmd-arg}))
        {:keys [union-branch member-schema]} ret]
    (-> (get-add-crdt-ops (assoc arg
                                 :path path
                                 :schema member-schema))
        ((fn [crdt-ops]
           (xf-op-paths {:prefix union-branch
                         :crdt-ops crdt-ops
                         :union? true}))))))

(defmethod get-add-crdt-ops :array
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt make-id path schema sys-time-ms]
    :as arg}]
  (let [items-schema (l/child-schema schema)
        ordered-node-ids (array/get-ordered-node-ids arg)]
    (if (and (= :zeno/set cmd-type) (seq path))
      (let [[i & sub-path] path
            ni (u/get-normalized-array-index
                {:array-len (count ordered-node-ids)
                 :i i})
            _ (when (or (not ni) (empty? ordered-node-ids))
                (throw (ex-info
                        (str "Index `" ni "` into array `" ordered-node-ids
                             "` is out of bounds.")
                        (u/sym-map cmd-arg cmd-path path
                                   ordered-node-ids i sub-path))))
            k (nth ordered-node-ids ni)
            add-crdt-ops (get-add-crdt-ops (assoc arg
                                        :crdt (get-in crdt [:children k])
                                        :path sub-path
                                        :schema items-schema))]
        (xf-op-paths {:array? true
                      :prefix k
                      :i ni
                      :crdt-ops add-crdt-ops}))
      (let [_ (when (and (= :zeno/set cmd-type) (not (sequential? cmd-arg)))
                (throw (ex-info
                        (str "Command path indicates an array, but arg is "
                             "not sequential. Got `" (or cmd-arg "nil") "`.")
                        (u/sym-map cmd path))))
            info (reduce
                  (fn [acc v]
                    (let [node-id (make-id)
                          add-crdt-ops (get-add-crdt-ops
                                   (assoc arg
                                          :cmd-arg v
                                          :crdt (get-in crdt
                                                        [:children node-id])
                                          :path []
                                          :schema items-schema))]
                      (-> acc
                          (update :node-crdt-ops
                                  (fn [node-crdt-ops]
                                    (set/union
                                     node-crdt-ops
                                     (xf-op-paths {:array? true
                                                   :prefix node-id
                                                   :i (count (:node-ids acc))
                                                   :crdt-ops add-crdt-ops}))))
                          (update :node-ids conj node-id))))
                  {:node-ids []
                   :node-crdt-ops #{}}
                  (if (sequential? cmd-arg) cmd-arg [cmd-arg]))
            {:keys [node-ids node-crdt-ops]} info
            initial-eops (if (empty? node-ids)
                           #{{:add-id (make-id)
                              :op-path '()
                              :op-type :add-array-edge
                              :sys-time-ms (or sys-time-ms (u/current-time-ms))
                              :value {:head-node-id array/array-start-node-id
                                      :tail-node-id array/array-end-node-id}}}
                           #{{:add-id (make-id)
                              :op-path '()
                              :op-type :add-array-edge
                              :sys-time-ms (or sys-time-ms (u/current-time-ms))
                              :value {:head-node-id array/array-start-node-id
                                      :tail-node-id (first node-ids)}}
                             {:add-id (make-id)
                              :op-path '()
                              :op-type :add-array-edge
                              :sys-time-ms (or sys-time-ms (u/current-time-ms))
                              :value {:head-node-id (last node-ids)
                                      :tail-node-id array/array-end-node-id}}})
            edge-crdt-ops (reduce (fn [acc [head-node-id tail-node-id]]
                               (conj acc
                                     {:add-id (make-id)
                                      :op-path '()
                                      :op-type :add-array-edge
                                      :sys-time-ms (or sys-time-ms
                                                       (u/current-time-ms))
                                      :value (u/sym-map head-node-id
                                                        tail-node-id)}))
                             initial-eops
                             (partition 2 1 node-ids))]
        (set/union node-crdt-ops edge-crdt-ops)))))

(defn check-insert-arg
  [{:keys [cmd-arg cmd-path cmd-type path schema] :as arg}]
  (when-not (= :array (l/schema-type schema))
    (throw (ex-info (str "`" cmd-type "` can only be used on array "
                         "schemas. Path indicates a schema of type `"
                         (l/schema-type schema) "`.")
                    (u/sym-map path cmd-arg cmd-type cmd-path))))
  (let [[i & sub-path] path]
    (when (and (seq sub-path) (not (int? i)))
      (throw (ex-info
              (str "In " cmd-type " update expressions, the path "
                   "must end with an array index. Got `" path "`.")
              (u/sym-map path cmd-type cmd-arg cmd-path))))
    (when-not (int? i)
      (throw (ex-info
              (str "In " cmd-type " update expressions, the last element "
                   "of the path must be an integer, e.g. [:x :y -1] "
                   " or [:a :b :c 12]. Got: `" i "`.")
              (u/sym-map path i cmd-type cmd-arg cmd-path))))))

(defn do-single-insert
  [{:keys [cmd-arg cmd-type make-id path schema]
    :as arg}]
  (check-insert-arg arg)
  (let [{repair-crdt-ops :crdt-ops
         repaired-crdt :crdt} (do-repair arg)
        [i & sub-path] path
        items-schema (l/child-schema schema)
        node-id (make-id)
        add-crdt-ops (get-add-crdt-ops
                 (assoc arg
                        :crdt repaired-crdt
                        :path sub-path
                        :schema items-schema))
        ordered-node-ids (array/get-ordered-node-ids arg)
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-crdt-ops (array/get-edge-crdt-ops-for-insert
                  (assoc arg
                         :crdt repaired-crdt
                         :i clamped-i
                         :new-node-id node-id
                         :ordered-node-ids ordered-node-ids))
        crdt-ops (set/union edge-crdt-ops (xf-op-paths
                                 {:array? true
                                  :prefix node-id
                                  :i (inc clamped-i)
                                  :crdt-ops add-crdt-ops}))
        final-crdt (apply-ops/apply-ops
                    (assoc arg
                           :crdt repaired-crdt
                           :schema schema
                           :crdt-ops crdt-ops))]
    {:crdt final-crdt
     :crdt-ops (set/union repair-crdt-ops crdt-ops)}))

(defn do-range-insert
  [{:keys [cmd cmd-arg cmd-type crdt make-id path schema]
    :as arg}]
  (check-insert-arg arg)
  (when-not (sequential? cmd-arg)
    (throw (ex-info (str "The `:zeno/arg` value in a `" (:zeno/op cmd)
                         "` command must be a sequence. Got `" cmd-arg "`.")
                    cmd)))
  (let [{repair-crdt-ops :crdt-ops
         repaired-crdt :crdt} (do-repair arg)
        [i & sub-path] path
        items-schema (l/child-schema schema)
        info (reduce
              (fn [acc v]
                (let [node-id (make-id)
                      add-crdt-ops (get-add-crdt-ops
                               (assoc arg
                                      :cmd-arg v
                                      :crdt (get-in crdt
                                                    [:children node-id])
                                      :path []
                                      :schema items-schema))]
                  (-> acc
                      (update :node-crdt-ops (fn [node-crdt-ops]
                                          (set/union
                                           node-crdt-ops
                                           (xf-op-paths
                                            {:array? true
                                             :prefix node-id
                                             :i (count (:new-node-ids acc))
                                             :crdt-ops add-crdt-ops}))))
                      (update :new-node-ids conj node-id))))
              {:new-node-ids []
               :node-crdt-ops #{}}
              cmd-arg)
        {:keys [new-node-ids node-crdt-ops]} info
        ordered-node-ids (array/get-ordered-node-ids arg)
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-crdt-ops (array/get-edge-crdt-ops-for-insert
                  (assoc arg
                         :crdt repaired-crdt
                         :i clamped-i
                         :new-node-ids new-node-ids
                         :ordered-node-ids ordered-node-ids))
        final-crdt (apply-ops/apply-ops
                    (assoc arg
                           :crdt repaired-crdt
                           :schema schema
                           :crdt-ops (set/union node-crdt-ops edge-crdt-ops)))]
    {:crdt final-crdt
     :crdt-ops (set/union repair-crdt-ops node-crdt-ops edge-crdt-ops)}))

(defmethod do-insert :array
  [{:keys [cmd-type] :as arg}]
  (case cmd-type
    :zeno/insert-after (do-single-insert arg)
    :zeno/insert-before (do-single-insert arg)
    :zeno/insert-range-after (do-range-insert arg)
    :zeno/insert-range-before (do-range-insert arg)))

(defn associative-do-insert
  [{:keys [cmd cmd-arg cmd-type path crdt schema]
    :as arg}]
  (when (empty? path)
    (let [schema-type (l/schema-type schema)]
      (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                           "an array. Path points to schema type `"
                           schema-type "`.")
                      (u/sym-map cmd path)))))
  (let [[k & ks] path
        _ (c/check-key (assoc arg :key k))
        child-schema (l/schema-at-path schema [k])
        child-crdt (get-in crdt [:children k])
        ret (do-insert (assoc arg
                              :crdt child-crdt
                              :path ks
                              :schema child-schema))
        new-crdt (assoc-in crdt [:children k] (:crdt ret))]
    {:crdt new-crdt
     :crdt-ops (xf-op-paths {:prefix k
                        :crdt-ops (:crdt-ops ret)})}))

(defmethod do-insert :map
  [{:keys [schema] :as arg}]
  (associative-do-insert arg))

(defmethod do-insert :record
  [{:keys [schema] :as arg}]
  (associative-do-insert arg))

(defmethod do-insert :union
  [{:keys [crdt path schema] :as arg}]
  (let [ret (c/get-union-branch-and-schema-for-key {:k (first path)
                                                    :schema schema})
        {:keys [member-schema union-branch]} ret]
    (-> (assoc arg :schema member-schema)
        (do-insert)
        (update :crdt-ops (fn [crdt-ops]
                       (xf-op-paths {:prefix union-branch
                                     :crdt-ops crdt-ops
                                     :union? true})))
        (assoc-in [:crdt :union-branch] union-branch))))

(defmethod do-insert :single-value
  [{:keys [cmd cmd-type schema] :as arg}]
  (let [schema-type (l/schema-type schema)]
    (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                         "an array. Path points to schema type `"
                         schema-type "`.")
                    cmd))))

(defmethod do-repair :array
  [arg]
  (let [{:keys [linear?]} (array/get-array-info arg)]
    (if linear?
      (assoc arg :crdt-ops #{})
      (array/repair-array arg))))

(defmethod do-repair :default
  [arg]
  (assoc arg :crdt-ops #{}))

(defmethod process-cmd* :zeno/set
  [{:keys [cmd-arg cmd-type] :as arg}]
  (let [{repair-crdt-ops :crdt-ops
         repaired-crdt :crdt} (do-repair arg)
        arg0 (assoc arg :crdt repaired-crdt)
        del-crdt-ops (get-delete-crdt-ops arg0)
        crdt1 (apply-ops/apply-ops (assoc arg0 :crdt-ops del-crdt-ops))
        arg1 (assoc arg0 :crdt crdt1)
        add-crdt-ops (get-add-crdt-ops arg1)
        crdt2 (apply-ops/apply-ops (assoc arg1 :crdt-ops add-crdt-ops))]
    {:crdt crdt2
     :crdt-ops (set/union (:crdt-ops arg)
                          repair-crdt-ops
                          del-crdt-ops
                          add-crdt-ops)}))

(defmethod process-cmd* :zeno/remove
  [{:keys [cmd-type] :as arg}]
  (let [{repair-crdt-ops :crdt-ops
         repaired-crdt :crdt} (do-repair arg)
        arg0 (assoc arg :crdt repaired-crdt)
        del-crdt-ops (get-delete-crdt-ops arg0)]
    {:crdt (apply-ops/apply-ops (assoc arg0 :crdt-ops del-crdt-ops))
     :crdt-ops (set/union (:crdt-ops arg) repair-crdt-ops del-crdt-ops)}))

(defmethod process-cmd* :zeno/insert*
  [arg]
  (do-insert arg))

(defn process-cmd [{:keys [cmd root] :as arg}]
  (let [cmd-path (:zeno/path cmd)]
    (process-cmd* (-> arg
                      (assoc :cmd-arg (:zeno/arg cmd))
                      (assoc :cmd-path cmd-path)
                      (assoc :cmd-type (:zeno/op cmd))
                      (assoc :path (u/chop-root cmd-path root))))))

(defn process-cmds [{:keys [cmds crdt make-id root] :as arg}]
  (let [{:keys [repair-crdt-ops]} crdt
        make-id (or make-id u/compact-random-uuid)
        result (reduce (fn [acc cmd]
                         (let [ret (process-cmd (assoc acc
                                                       :cmd cmd
                                                       :make-id make-id))]
                           (-> acc
                               (assoc :crdt (:crdt ret))
                               (update :crdt-ops set/union (:crdt-ops ret))
                               (update :updated-paths conj (:zeno/path cmd)))))
                       arg
                       cmds)]
    (-> result
        (update :crdt dissoc :repair-crdt-ops)
        (update :crdt-ops set/union repair-crdt-ops))
    #_(update result :crdt-ops vec)))
