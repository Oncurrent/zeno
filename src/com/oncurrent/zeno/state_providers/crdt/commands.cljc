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

(defmulti get-delete-info (fn [{:keys [schema]}]
                            (c/schema->dispatch-type schema)))

(defmulti get-add-info (fn [{:keys [schema]}]
                             (c/schema->dispatch-type schema)))

(defn xf-op-paths [{:keys [prefix crdt-ops]}]
  (reduce (fn [acc op]
            (conj acc (update op :op-path #(cons prefix %))))
          #{}
          crdt-ops))

(defmethod get-delete-info :single-value
  [arg]
  (let [{:keys [current-add-id-to-value-info]} (:crdt arg)
        crdt-ops (reduce-kv (fn [acc add-id value-info]
                              (conj acc {:add-id add-id
                                         :op-path '()
                                         :op-type :delete-value}))
                            #{}
                            current-add-id-to-value-info)
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defn associative-get-delete-info
  [{:keys [crdt get-child-path-info get-child-schema path schema array?]
    :as arg}]
  (let [get-child-del-info (fn [{:keys [k i sub-path] :as gcdi-arg}]
                             (let [info (get-delete-info
                                         (assoc arg
                                                :crdt (:crdt gcdi-arg)
                                                :path sub-path
                                                :schema (get-child-schema k)))
                                   crdt-ops (xf-op-paths
                                             (cond-> {:prefix k
                                                      :crdt-ops (:crdt-ops info)}
                                               i (assoc :i i :array? true)))]
                               (assoc info :crdt-ops crdt-ops)))]
    (if (empty? path)
      (reduce-kv (fn [acc k child-crdt]
                   (let [ret (get-child-del-info
                              (cond-> {:crdt child-crdt
                                       :k k
                                       :sub-path []}
                                array? (assoc :i 0)))
                         new-crdt (cond-> (:crdt ret)
                                    array? (assoc :ordered-node-ids []))]
                     (-> acc
                         (assoc-in [:crdt :children k] new-crdt)
                         (update :crdt-ops set/union (:crdt-ops ret)))))
                 {:crdt crdt
                  :crdt-ops (:crdt-ops arg)}
                 (:children crdt))
      (let [path-info (get-child-path-info path)
            ret (get-child-del-info path-info)]
        {:crdt (assoc-in crdt [:children (:k path-info)] (:crdt ret))
         :crdt-ops (:crdt-ops ret)}))))

(defmethod get-delete-info :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (associative-get-delete-info
     (assoc arg
            :get-child-path-info (fn [[k & sub-path]]
                                   (let [crdt (get-in arg [:crdt :children k])]
                                     (u/sym-map crdt k sub-path)))
            :get-child-schema (constantly values-schema)))))

(defmethod get-delete-info :record
  [{:keys [cmd crdt path schema] :as arg}]
  (associative-get-delete-info
   (assoc arg
          :get-child-path-info (fn [[k & sub-path]]
                                 (let [crdt (get-in arg [:crdt :children k])]
                                   (u/sym-map crdt k sub-path)))
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

(defmethod get-delete-info :array
  [{:keys [make-id cmd crdt path schema sys-time-ms] :as arg}]
  (let [items-schema (l/child-schema schema)
        {:keys [add-id-to-edge current-edge-add-ids]} crdt
        ordered-node-ids (or (:ordered-node-ids crdt) [])
        arg* (assoc arg :get-child-schema (constantly items-schema))
        del-info (associative-get-delete-info
                  (assoc arg*
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
                                      :i i})
                                 child-id (nth ordered-node-ids ni)]
                             (when (or (not ni) (empty? ordered-node-ids))
                               (throw
                                (ex-info
                                 (str "Index `" ni "` into array `"
                                      ordered-node-ids "` is out of bounds.")
                                 (u/sym-map cmd path ordered-node-ids
                                            ni i sub-path))))
                             {:crdt (get-in crdt [:children child-id])
                              :k (nth ordered-node-ids ni)
                              :i i
                              :sub-path sub-path}))))
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
                        (do
                          (when (or (> i (count ordered-node-ids))
                                    (empty? ordered-node-ids))
                            (throw (ex-info
                                    (str "Index `" i "` into array `"
                                         ordered-node-ids "` is out of bounds.")
                                    (u/sym-map cmd path ordered-node-ids
                                               add-id-to-edge
                                               current-edge-add-ids
                                               i sub-path))))
                          (get-crdt-ops-del-single-node
                           (assoc (u/sym-map crdt make-id sys-time-ms)
                                  :node-id (nth ordered-node-ids i))))

                        :else
                        #{})
        crdt-ops (set/union (:crdt-ops arg)
                            (:crdt-ops del-info)
                            edge-crdt-ops)
        crdt* (:crdt del-info)
        array-info (array/get-array-info (assoc arg* :crdt crdt*))]
    {:crdt (assoc crdt* :ordered-node-ids (:ordered-node-ids array-info))
     :crdt-ops crdt-ops}))

(defmethod get-delete-info :union
  [{:keys [crdt path schema] :as arg}]
  (let [{:keys [union-branch]} crdt
        member-schema (when union-branch
                        (l/member-schema-at-branch schema union-branch))]
    (if-not member-schema
      (assoc arg :crdt-ops #{})
      (let [del-info (get-delete-info (assoc arg
                                             :path path
                                             :schema member-schema))]
        (update del-info :crdt-ops
                #(set/union (:crdt-ops arg)
                            (xf-op-paths {:prefix union-branch
                                          :crdt-ops %
                                          :union? true})))))))

(defmethod get-add-info :single-value
  [{:keys [cmd-arg make-id sys-time-ms] :as arg}]
  (let [crdt-ops #{{:add-id (make-id)
                    :op-path '()
                    :op-type :add-value
                    :sys-time-ms (or sys-time-ms (u/current-time-ms))
                    :value cmd-arg}}
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defmethod get-add-info :map
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt path schema] :as arg}]
  (let [values-schema (l/child-schema schema)]
    (if (seq path)
      (let [[k & ks] path
            add-info (get-add-info (assoc arg
                                          :crdt (get-in crdt [:children k])
                                          :path ks
                                          :schema values-schema))
            crdt-ops (set/union (:crdt-ops arg)
                                (xf-op-paths {:prefix k
                                              :crdt-ops (:crdt-ops add-info)}))]
        {:crdt (assoc-in crdt [:children k] (:crdt add-info))
         :crdt-ops crdt-ops})
      (let [edn-schema (l/edn schema)
            pred (c/edn-schema->pred edn-schema)
            _ (when (and (= :zeno/set cmd-type) (not (pred cmd-arg)))
                (throw (ex-info
                        (str ":arg (`" (or cmd-arg "nil") "`) is not the "
                             "correct type for this command.")
                        (u/sym-map cmd path))))]
        (reduce-kv (fn [acc k v]
                     (let [add-info (get-add-info
                                     (assoc arg
                                            :cmd-arg v
                                            :crdt (get-in (:crdt acc)
                                                          [:children k])
                                            :path []
                                            :schema values-schema))
                           crdt-ops (set/union
                                     (:crdt-ops acc)
                                     (xf-op-paths
                                      {:prefix k
                                       :crdt-ops (:crdt-ops add-info)}))]
                       (-> acc
                           (assoc-in [:crdt :children k] (:crdt add-info))
                           (assoc :crdt-ops crdt-ops))))
                   {:crdt (or crdt {})
                    :crdt-ops (:crdt-ops arg)}
                   cmd-arg)))))

(defmethod get-add-info :record
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt path schema] :as arg}]
  (if (seq path)
    (let [[k & ks] path
          add-info (get-add-info (assoc arg
                                        :crdt (get-in crdt [:children k])
                                        :path ks
                                        :schema (l/child-schema schema k)))
          crdt-ops (set/union (:crdt-ops arg)
                              (xf-op-paths {:prefix k
                                            :crdt-ops (:crdt-ops add-info)}))]
      {:crdt (assoc-in crdt [:children k] (:crdt add-info))
       :crdt-ops crdt-ops})
    (do
      (when (and (= :zeno/set cmd-type) (not (associative? cmd-arg)))
        (throw (ex-info
                (str "Command path indicates a record, but `:arg` is "
                     "not associative. Got `" (or cmd-arg "nil") "`.")
                (u/sym-map cmd path))))
      (reduce (fn [acc k]
                (let [v (get cmd-arg k)
                      child-schema (l/child-schema schema k)
                      child-crdt (get-in (:crdt acc) [:children k])
                      add-info (get-add-info
                                (assoc arg
                                       :cmd-arg v
                                       :crdt child-crdt
                                       :path []
                                       :schema child-schema))
                      crdt-ops (when add-info
                                 (set/union (:crdt-ops acc)
                                            (xf-op-paths
                                             {:prefix k
                                              :crdt-ops (:crdt-ops add-info)})))]
                  (if-not add-info
                    acc
                    (-> acc
                        (assoc-in [:crdt :children k] (:crdt add-info))
                        (assoc :crdt-ops crdt-ops)))))
              {:crdt crdt
               :crdt-ops (:crdt-ops arg)}
              (->> (l/edn schema)
                   (:fields)
                   (map :name))))))

(defmethod get-add-info :union
  [{:keys [cmd-arg path schema] :as arg}]
  (let [ret (if (seq path)
              (c/get-union-branch-and-schema-for-key {:schema schema
                                                      :k (first path)})
              (c/get-union-branch-and-schema-for-value {:schema schema
                                                        :v cmd-arg}))
        {:keys [union-branch member-schema]} ret
        add-info (get-add-info (assoc arg
                                      :path path
                                      :schema member-schema))
        crdt-ops (xf-op-paths {:prefix union-branch
                               :crdt-ops (:crdt-ops add-info)
                               :union? true})]
    (-> add-info
        (assoc-in [:crdt :union-branch] union-branch)
        (assoc :crdt-ops crdt-ops))))

(defmethod get-add-info :array
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt make-id path schema sys-time-ms]
    :as arg}]
  (let [items-schema (l/child-schema schema)
        arg* (assoc arg :get-child-schema (constantly items-schema))
        ordered-node-ids (or (:ordered-node-ids crdt) [])]
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
            add-info (get-add-info (assoc arg
                                          :crdt (get-in crdt [:children k])
                                          :path sub-path
                                          :schema items-schema))
            crdt-ops (set/union (:crdt-ops arg)
                                (xf-op-paths {:array? true
                                              :prefix k
                                              :i ni
                                              :crdt-ops (:crdt-ops add-info)}))
            crdt* (assoc-in crdt [:children k] (:crdt add-info))
            array-info (array/get-array-info (assoc arg* :crdt crdt*))]
        {:crdt (assoc crdt* :ordered-node-ids (:ordered-node-ids array-info))
         :crdt-ops crdt-ops})
      (let [_ (when (and (= :zeno/set cmd-type) (not (sequential? cmd-arg)))
                (throw (ex-info
                        (str "Command path indicates an array, but arg is "
                             "not sequential. Got `" (or cmd-arg "nil") "`.")
                        (u/sym-map cmd path))))
            info (reduce
                  (fn [acc v]
                    (let [node-id (make-id)
                          add-info (get-add-info
                                    (assoc arg
                                           :cmd-arg v
                                           :crdt (get-in (:crdt acc)
                                                         [:children node-id])
                                           :path []
                                           :schema items-schema))]
                      (-> acc
                          (assoc-in [:crdt :children node-id] (:crdt add-info))
                          (update :node-crdt-ops
                                  (fn [node-crdt-ops]
                                    (set/union
                                     node-crdt-ops
                                     (xf-op-paths {:array? true
                                                   :prefix node-id
                                                   :i (count (:node-ids acc))
                                                   :crdt-ops (:crdt-ops
                                                              add-info)}))))
                          (update :node-ids conj node-id))))
                  {:crdt crdt
                   :node-ids []
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
                                  (partition 2 1 node-ids))
            crdt-ops (set/union (:crdt-ops arg)
                                node-crdt-ops
                                edge-crdt-ops)
            crdt* (apply-ops/apply-ops (assoc arg
                                              :crdt (:crdt info)
                                              :schema schema
                                              :crdt-ops crdt-ops))
            array-info (array/get-array-info (assoc arg* :crdt crdt*))]
        {:crdt (assoc crdt* :ordered-node-ids (:ordered-node-ids array-info))
         :crdt-ops crdt-ops}))))

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
  [{:keys [cmd-arg cmd-type crdt make-id path schema]
    :as arg}]
  (check-insert-arg arg)
  (let [[i & sub-path] path
        items-schema (l/child-schema schema)
        node-id (make-id)
        ordered-node-ids (or (:ordered-node-ids crdt) [])
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-info (array/get-edge-insert-info
                   (assoc arg
                          :i clamped-i
                          :new-node-id node-id
                          :ordered-node-ids ordered-node-ids))
        add-info (get-add-info (assoc arg
                                      :path sub-path
                                      :schema items-schema))
        edge-ops (:crdt-ops edge-info)
        crdt-ops (set/union (:crdt-ops arg)
                            edge-ops
                            (xf-op-paths {:array? true
                                          :prefix node-id
                                          :i (inc clamped-i)
                                          :crdt-ops (:crdt-ops add-info)}))
        crdt* (assoc-in crdt [:children node-id] (:crdt add-info))
        final-crdt (-> (apply-ops/apply-ops (assoc arg
                                                   :crdt crdt*
                                                   :crdt-ops edge-ops))
                       (assoc :ordered-node-ids (:ordered-node-ids edge-info)))]
    {:crdt final-crdt
     :crdt-ops crdt-ops}))

(defn do-range-insert
  [{:keys [cmd cmd-arg cmd-type crdt make-id path schema]
    :as arg}]
  (check-insert-arg arg)
  (when-not (sequential? cmd-arg)
    (throw (ex-info (str "The `:zeno/arg` value in a `" (:zeno/op cmd)
                         "` command must be a sequence. Got `" cmd-arg "`.")
                    cmd)))
  (let [[i & sub-path] path
        items-schema (l/child-schema schema)
        info (reduce
              (fn [acc v]
                (let [node-id (make-id)
                      add-info (get-add-info
                                (assoc arg
                                       :cmd-arg v
                                       :crdt (get-in (:crdt acc)
                                                     [:children node-id])
                                       :path []
                                       :schema items-schema))]
                  (-> acc
                      (assoc-in [:crdt :children node-id] (:crdt add-info))
                      (update :node-crdt-ops (fn [node-crdt-ops]
                                               (set/union
                                                node-crdt-ops
                                                (xf-op-paths
                                                 {:array? true
                                                  :prefix node-id
                                                  :i (count (:new-node-ids acc))
                                                  :crdt-ops (:crdt-ops
                                                             add-info)}))))
                      (update :new-node-ids conj node-id))))
              {:crdt crdt
               :new-node-ids []
               :node-crdt-ops #{}}
              cmd-arg)
        {:keys [new-node-ids node-crdt-ops]} info
        ordered-node-ids (or (:ordered-node-ids crdt) [])
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-info (array/get-edge-insert-info
                   (assoc arg
                          :i clamped-i
                          :new-node-ids new-node-ids
                          :ordered-node-ids ordered-node-ids))
        edge-ops (:crdt-ops edge-info)
        crdt-ops (set/union (:crdt-ops arg)
                            node-crdt-ops
                            edge-ops)
        final-crdt (-> (apply-ops/apply-ops (assoc arg
                                                   :crdt (:crdt info)
                                                   :schema schema
                                                   :crdt-ops edge-ops))
                       (assoc :ordered-node-ids (:ordered-node-ids edge-info)))]
    {:crdt final-crdt
     :crdt-ops crdt-ops}))

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

(defmethod process-cmd* :zeno/set
  [arg]
  (let [del-info (get-delete-info arg)
        arg1 (assoc arg :crdt (:crdt del-info))
        add-info (get-add-info arg1)]
    {:crdt (:crdt add-info)
     :crdt-ops (set/union (:crdt-ops arg)
                          (:crdt-ops del-info)
                          (:crdt-ops add-info))}))

(defmethod process-cmd* :zeno/remove
  [arg]
  (get-delete-info arg))

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
  (let [make-id* (or make-id u/compact-random-uuid)]
    (reduce (fn [acc cmd]
              (let [ret (process-cmd (assoc acc
                                            :cmd cmd
                                            :make-id make-id*))]
                (-> acc
                    (assoc :crdt (:crdt ret))
                    (update :crdt-ops set/union (:crdt-ops ret)))))
            arg
            cmds)))
