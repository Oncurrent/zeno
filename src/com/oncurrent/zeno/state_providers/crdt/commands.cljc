(ns com.oncurrent.zeno.state-providers.crdt.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.array :as array]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def insert-ops #{:zeno/insert-after
                  :zeno/insert-before
                  :zeno/insert-range-after
                  :zeno/insert-range-before})

(defmulti process-cmd* (fn [{:keys [cmd]}]
                         (let [{:zeno/keys [op]} cmd]
                           (if (insert-ops op)
                             :zeno/insert*
                             op))))

(defmulti do-insert (fn [{:keys [schema]}]
                      (c/schema->dispatch-type schema)))

(defmulti do-repair (fn [{:keys [schema]}]
                      (l/schema-type schema)))

(defmulti get-delete-ops (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmulti get-add-ops (fn [{:keys [schema]}]
                             (c/schema->dispatch-type schema)))

(defn xf-op-paths [{:keys [prefix i ops union? array?]}]
  (reduce (fn [acc {:keys [norm-path] :as op}]
            (conj acc
                  (cond-> op
                    true
                    (update :path #(cons prefix %))

                    (and norm-path
                         (not array?)
                         (not union?))
                    (update :norm-path #(cons prefix %))

                    (and norm-path
                         array?
                         (not union?))
                    (update :norm-path #(cons i %)))))
          #{}
          ops))

(defmethod get-delete-ops :single-value
  [{:keys [crdt]}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (reduce-kv (fn [acc add-id value-info]
                 (conj acc {:add-id add-id
                            :op-type :delete-value
                            :path '()
                            :norm-path '()}))
               #{}
               current-add-id-to-value-info)))

(defn associative-get-delete-ops
  [{:keys [crdt get-child-path-info get-child-schema
           norm-path path schema array?]
    :as arg}]
  (let [get-child-ops (fn [{:keys [k i sub-path]}]
                        (-> (get-delete-ops
                             (assoc arg
                                    :crdt (get-in crdt [:children k])
                                    :path sub-path
                                    :schema (get-child-schema k)))
                            ((fn [ops]
                               (xf-op-paths
                                (cond-> {:prefix k
                                         :ops ops}
                                  i (assoc :i i :array? true)))))))]
    (if (empty? path)
      (do
       (reduce-kv (fn [acc k _]
                    (let [ret (get-child-ops (cond-> {:k k
                                                      :sub-path []}
                                               array? (assoc :i 0)))]
                     (set/union acc ret)))
                 #{}
                 (:children crdt)))
      (-> path
          (get-child-path-info)
          (get-child-ops)))))

(defmethod get-delete-ops :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/schema-at-path schema ["x"])]
    (associative-get-delete-ops
     (assoc arg
            :get-child-path-info (fn [[k & sub-path]]
                                   (u/sym-map k sub-path))
            :get-child-schema (constantly values-schema)))))

(defmethod get-delete-ops :record
  [{:keys [path schema] :as arg}]
  (associative-get-delete-ops
   (assoc arg
          :get-child-path-info (fn [[k & sub-path]]
                                 (u/sym-map k sub-path))
          :get-child-schema (fn [k]
                              (or (l/schema-at-path schema [k])
                                  (throw (ex-info (str "Bad record key `" k
                                                       "` in path `"
                                                       path "`.")
                                                  (u/sym-map path k))))))))

(defn get-ops-del-single-node
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
       :op-type :delete-array-edge
       :path '()}
      {:add-id out-add-id
       :op-type :delete-array-edge
       :path '()}
      {:add-id (make-id)
       :op-type :add-array-edge
       :path '()
       :sys-time-ms (or sys-time-ms (u/current-time-ms))
       :value {:head-node-id (:head-node-id (add-id-to-edge in-add-id))
               :tail-node-id (:tail-node-id (add-id-to-edge out-add-id))}}}))

(defmethod get-delete-ops :array
  [{:keys [make-id cmd crdt path schema sys-time-ms] :as arg}]
  (let [items-schema (l/schema-at-path schema [0])
        ordered-node-ids (array/get-ordered-node-ids arg)
        node-ops (associative-get-delete-ops
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
        edge-ops (cond
                   (empty? path)
                   (reduce (fn [acc eaid]
                             (conj acc {:add-id eaid
                                        :op-type :delete-array-edge
                                        :path '()}))
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
                     (get-ops-del-single-node (u/sym-map crdt make-id node-id
                                                         sys-time-ms)))

                   :else
                   #{})]
    (set/union node-ops edge-ops)))

(defmethod get-delete-ops :union
  [{:keys [crdt norm-path path schema] :as arg}]
  (let [{:keys [union-branch]} crdt
        member-schema (when union-branch
                        (l/member-schema-at-branch schema union-branch))]
    (if member-schema
      (-> (get-delete-ops (assoc arg
                                 :path path
                                 :schema member-schema))
          ((fn [ops]
             (xf-op-paths {:prefix union-branch
                           :ops ops
                           :union? true}))))
      #{})))

(defmethod get-add-ops :single-value
  [{:keys [cmd-arg make-id norm-path sys-time-ms]}]
  #{{:add-id (make-id)
     :norm-path '()
     :op-type :add-value
     :path '()
     :sys-time-ms (or sys-time-ms (u/current-time-ms))
     :value cmd-arg}})

(defmethod get-add-ops :map
  [{:keys [cmd-arg cmd-path crdt norm-path path schema] :as arg}]
  (let [values-schema (l/schema-at-path schema ["x"])]
    (if (seq path)
      (let [[k & ks] path]
        (-> (get-add-ops (assoc arg
                                :crdt (get-in crdt [:children k])
                                :norm-path (conj norm-path k)
                                :path ks
                                :schema values-schema))
            ((fn [ops]
               (xf-op-paths {:prefix k
                             :ops ops})))))
      (let [edn-schema (l/edn schema)
            pred (c/edn-schema->pred edn-schema)
            _ (when-not (pred cmd-arg)
                (throw (ex-info
                        (str ":arg (` " (or cmd-arg "nil") "`) is not the "
                             "correct type for this command.")
                        {:arg cmd-arg
                         :path cmd-path})))]
        (reduce-kv (fn [acc k v]
                     (let [add-ops (get-add-ops
                                    (assoc arg
                                           :cmd-arg v
                                           :crdt (get-in crdt [:children k])
                                           :path []
                                           :schema values-schema))]
                       (set/union acc
                                  (xf-op-paths {:prefix k
                                                :ops add-ops}))))
                   #{}
                   cmd-arg)))))

(defmethod get-add-ops :record
  [{:keys [cmd-arg cmd-path crdt norm-path path schema] :as arg}]
  (if (seq path)
    (let [[k & ks] path]
      (-> (get-add-ops (assoc arg
                                   :crdt (get-in crdt [:children k])
                                   :norm-path (conj norm-path k)
                                   :path ks
                                   :schema (l/schema-at-path schema [k])))
          ((fn [ops]
             (xf-op-paths {:prefix k
                           :ops ops})))))
    (do
      (when-not (associative? cmd-arg)
        (throw (ex-info
                (str "Command path indicates a record, but `:arg` is "
                     "not associative. Got `" (or cmd-arg "nil") "`.")
                {:arg cmd-arg
                 :path cmd-path})))
      (reduce (fn [acc k]
                (let [v (get cmd-arg k)
                      child-schema (l/schema-at-path schema [k])
                      add-ops (get-add-ops
                           (assoc arg
                                  :cmd-arg v
                                  :crdt (get-in crdt [:children k])
                                  :norm-path (conj norm-path k)
                                  :path []
                                  :schema child-schema))]
                  (set/union acc
                             (xf-op-paths {:prefix k
                                           :ops add-ops}))))
              #{}
              (->> (l/edn schema)
                   (:fields)
                   (map :name))))))

(defmethod get-add-ops :union
  [{:keys [cmd-arg path schema] :as arg}]
  (let [ret (if (seq path)
              (c/get-union-branch-and-schema-for-key {:schema schema
                                                      :k (first path)})
              (c/get-union-branch-and-schema-for-value {:schema schema
                                                        :v cmd-arg}))
        {:keys [union-branch member-schema]} ret]
    (-> (get-add-ops (assoc arg
                                 :path path
                                 :schema member-schema))
        ((fn [ops]
           (xf-op-paths {:prefix union-branch
                         :ops ops
                         :union? true}))))))

(defmethod get-add-ops :array
  [{:keys [cmd-arg cmd-path crdt make-id norm-path path
           schema sys-time-ms]
    :as arg}]
  (let [items-schema (l/schema-at-path schema [0])
        ordered-node-ids (array/get-ordered-node-ids arg)]
    (if (seq path)
      (let [[i & sub-path] path
            ni (u/get-normalized-array-index
                {:array-len (count ordered-node-ids)
                 :i i})
            _ (when (or (not ni) (empty? ordered-node-ids))
                (throw (ex-info
                        (str "Index `" ni "` into array `" ordered-node-ids
                             "` is out of bounds.")
                        (u/sym-map cmd-arg cmd-path norm-path path
                                   ordered-node-ids i sub-path))))
            k (nth ordered-node-ids ni)
            add-ops (get-add-ops (assoc arg
                                        :crdt (get-in crdt [:children k])
                                        :path sub-path
                                        :schema items-schema))]
        (xf-op-paths {:array? true
                      :prefix k
                      :i ni
                      :ops add-ops}))
      (let [_ (when-not (sequential? cmd-arg)
                (throw (ex-info
                        (str "Command path indicates an array, but arg is "
                             "not sequential. Got `" (or cmd-arg "nil") "`.")
                        {:arg cmd-arg
                         :path cmd-path})))
            info (reduce
                  (fn [acc v]
                    (let [node-id (make-id)
                          add-ops (get-add-ops
                                   (assoc arg
                                          :cmd-arg v
                                          :crdt (get-in crdt
                                                        [:children node-id])
                                          :path []
                                          :schema items-schema))]
                      (-> acc
                          (update :node-ops
                                  (fn [node-ops]
                                    (set/union
                                     node-ops
                                     (xf-op-paths {:array? true
                                                   :prefix node-id
                                                   :i (count (:node-ids acc))
                                                   :ops add-ops}))))
                          (update :node-ids conj node-id))))
                  {:node-ids []
                   :node-ops #{}}
                  cmd-arg)
            {:keys [node-ids node-ops]} info
            initial-eops (if (empty? node-ids)
                           #{}
                           #{{:add-id (make-id)
                              :op-type :add-array-edge
                              :path '()
                              :sys-time-ms (or sys-time-ms (u/current-time-ms))
                              :value {:head-node-id array/array-start-node-id
                                      :tail-node-id (first node-ids)}}
                             {:add-id (make-id)
                              :op-type :add-array-edge
                              :path '()
                              :sys-time-ms (or sys-time-ms (u/current-time-ms))
                              :value {:head-node-id (last node-ids)
                                      :tail-node-id array/array-end-node-id}}})
            edge-ops (reduce (fn [acc [head-node-id tail-node-id]]
                               (conj acc
                                     {:add-id (make-id)
                                      :op-type :add-array-edge
                                      :path '()
                                      :sys-time-ms (or sys-time-ms
                                                       (u/current-time-ms))
                                      :value (u/sym-map head-node-id
                                                        tail-node-id)}))
                             initial-eops
                             (partition 2 1 node-ids))]
        (set/union node-ops edge-ops)))))

(defn check-insert-arg
  [{:keys [cmd-arg cmd-path cmd-type path schema] :as arg}]
  (when-not (= :array (l/schema-type schema))
    (throw (ex-info (str "`" cmd-type "` can only be used on array "
                         "schemas. Path indicates a schema of type `"
                         (l/schema-type schema) "`.")
                    (u/sym-map path cmd-arg cmd-type cmd-path))))
  (let [[i & sub-path] path]
    (when (seq sub-path)
      (throw (ex-info
              (str "In " cmd-type " update expressions, the path"
                   "must end with an array index. Got `" path "`.")
              (u/sym-map path cmd-type cmd-arg cmd-path))))
    (when-not (int? i)
      (throw (ex-info
              (str "In " cmd-type " update expressions, the last element "
                   "of the path must be an integer, e.g. [:x :y -1] "
                   " or [:a :b :c 12]. Got: `" i "`.")
              (u/sym-map path i cmd-type cmd-arg cmd-path))))))

(defn do-single-insert
  [{:keys [cmd-arg cmd-type make-id norm-path path schema]
    :as arg}]
  (check-insert-arg arg)
  (let [{repair-ops :ops
         repaired-crdt :crdt} (do-repair arg)
        [i & sub-path] path
        items-schema (l/schema-at-path schema [0])
        node-id (make-id)
        add-ops (get-add-ops
                 (assoc arg
                        :crdt repaired-crdt
                        :path []
                        :schema items-schema))
        ordered-node-ids (array/get-ordered-node-ids arg)
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-ops (array/get-edge-ops-for-insert
                  (assoc arg
                         :crdt repaired-crdt
                         :i clamped-i
                         :new-node-id node-id
                         :ordered-node-ids ordered-node-ids))
        ops (set/union edge-ops (xf-op-paths
                                 {:array? true
                                  :prefix node-id
                                  :i (inc clamped-i)
                                  :ops add-ops}))
        final-crdt (apply-ops/apply-ops
                    (assoc arg
                           :crdt repaired-crdt
                           :schema schema
                           :ops ops))]
    {:crdt final-crdt
     :ops (set/union repair-ops ops)
     :update-info {:norm-path norm-path
                   :op cmd-type
                   :value cmd-arg}}))

(defn do-range-insert
  [{:keys [cmd cmd-arg cmd-type crdt make-id norm-path path
           schema]
    :as arg}]
  (check-insert-arg arg)
  (when-not (sequential? cmd-arg)
    (throw (ex-info (str "The `:zeno/arg` value in a `" (:zeno/op cmd)
                         "` command must be a sequence. Got `" cmd-arg "`.")
                    cmd)))
  (let [{repair-ops :ops
         repaired-crdt :crdt} (do-repair arg)
        [i & sub-path] path
        items-schema (l/schema-at-path schema [0])
        info (reduce
              (fn [acc v]
                (let [node-id (make-id)
                      add-ops (get-add-ops
                               (assoc arg
                                      :cmd-arg v
                                      :crdt (get-in crdt
                                                    [:children node-id])
                                      :path []
                                      :schema items-schema))]
                  (-> acc
                      (update :node-ops (fn [node-ops]
                                          (set/union
                                           node-ops
                                           (xf-op-paths
                                            {:array? true
                                             :prefix node-id
                                             :i (count (:new-node-ids acc))
                                             :ops add-ops}))))
                      (update :new-node-ids conj node-id))))
              {:new-node-ids []
               :node-ops #{}}
              cmd-arg)
        {:keys [new-node-ids node-ops]} info
        ordered-node-ids (array/get-ordered-node-ids arg)
        array-len (count ordered-node-ids)
        clamped-i (u/get-clamped-array-index (u/sym-map array-len i))
        edge-ops (array/get-edge-ops-for-insert
                  (assoc arg
                         :crdt repaired-crdt
                         :i clamped-i
                         :new-node-ids new-node-ids
                         :ordered-node-ids ordered-node-ids))
        final-crdt (apply-ops/apply-ops
                    (assoc arg
                           :crdt repaired-crdt
                           :schema schema
                           :ops (set/union node-ops edge-ops)))]
    {:crdt final-crdt
     :ops (set/union repair-ops node-ops edge-ops)
     :update-info {:norm-path norm-path
                   :op cmd-type
                   :value cmd-arg}}))

(defmethod do-insert :array
  [{:keys [cmd-type] :as arg}]
  (case cmd-type
    :zeno/insert-after (do-single-insert arg)
    :zeno/insert-before (do-single-insert arg)
    :zeno/insert-range-after (do-range-insert arg)
    :zeno/insert-range-before (do-range-insert arg)))

(defn associative-do-insert
  [{:keys [get-child-schema cmd-type cmd-arg crdt norm-path path schema]
    :as arg}]
  (when (empty? path)
    (let [schema-type (l/schema-type schema)]
      (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                           "an array. Path points to schema type `"
                           schema-type "`.")
                      (u/sym-map schema-type cmd-type cmd-arg path)))))
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
     :ops (xf-op-paths {:prefix k
                        :ops (:ops ret)})
     :update-info {:norm-path norm-path
                   :op cmd-type
                   :value cmd-arg}}))

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
        (update :ops (fn [ops]
                       (xf-op-paths {:prefix union-branch
                                     :ops ops
                                     :union? true}))))))

(defmethod do-insert :single-value
  [{:keys [cmd-arg cmd-type schema path] :as arg}]
  (let [schema-type (l/schema-type schema)]
    (throw (ex-info (str "Can only process `" cmd-type "` cmd on "
                         "an array. Path points to schema type `"
                         schema-type "`.")
                    (u/sym-map schema-type cmd-type cmd-arg path)))))

(defmethod do-repair :array
  [arg]
  (let [{:keys [linear?]} (array/get-array-info arg)]
    (if linear?
      (assoc arg :ops #{})
      (array/repair-array arg))))

(defmethod do-repair :default
  [arg]
  (assoc arg :ops #{}))

(defmethod process-cmd* :zeno/set
  [{:keys [cmd-arg cmd-type norm-path path] :as arg}]
  (let [{repair-ops :ops
         repaired-crdt :crdt} (do-repair arg)
        arg0 (assoc arg :crdt repaired-crdt)
        del-ops (get-delete-ops arg0)
        crdt1 (apply-ops/apply-ops (assoc arg0 :ops del-ops))
        arg1 (assoc arg0 :crdt crdt1)
        add-ops (get-add-ops arg1)
        crdt2 (apply-ops/apply-ops (assoc arg1 :ops add-ops))]
    {:crdt crdt2
     :ops (set/union (:ops arg)
                     repair-ops
                     del-ops
                     add-ops)
     :update-info {:norm-path norm-path
                   :op cmd-type
                   :value cmd-arg}}))

(defmethod process-cmd* :zeno/remove
  [{:keys [cmd-type norm-path] :as arg}]
  (let [{repair-ops :ops
         repaired-crdt :crdt} (do-repair arg)
        arg0 (assoc arg :crdt repaired-crdt)
        del-ops (get-delete-ops arg0)]
    {:crdt (apply-ops/apply-ops (assoc arg0 :ops del-ops))
     :ops (set/union (:ops arg) repair-ops del-ops)
     :update-info {:norm-path norm-path
                   :op cmd-type
                   :value nil}}))

(defmethod process-cmd* :zeno/insert*
  [arg]
  (do-insert arg))

(defn process-cmd [{:keys [cmd] :as arg}]
  (let [path (-> cmd :zeno/path c/chop-root)]
    (process-cmd* (-> arg
                      (assoc :cmd-arg (:zeno/arg cmd))
                      (assoc :cmd-path path)
                      (assoc :cmd-type (:zeno/op cmd))
                      (assoc :norm-path path)
                      (assoc :path path)
                      (assoc :schema (:crdt-schema arg))))))

(defn process-cmds [{:keys [cmds make-id]
                     :or {make-id u/compact-random-uuid}
                     :as arg}]
  (reduce (fn [acc cmd]
            (let [ret (process-cmd (assoc acc
                                          :cmd cmd
                                          :make-id make-id))]
              (-> acc
                  (assoc :crdt (:crdt ret))
                  (update :ops set/union (:ops ret))
                  (update :update-infos conj (:update-info ret)))))
          arg
          cmds))
