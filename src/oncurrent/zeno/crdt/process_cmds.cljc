(ns oncurrent.zeno.crdt.process-cmds
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.array :as array]
   [oncurrent.zeno.crdt.common :as c]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defmulti process-cmd :cmd-type)

(defmulti get-delete-ops (fn [{:keys [cmd schema]}]
                           (c/schema->dispatch-type schema)))

(defmulti get-add-ops (fn [{:keys [cmd schema]}]
                        (c/schema->dispatch-type schema)))

(defmethod get-delete-ops :single-value
  [{:keys [crdt op-path]}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (reduce-kv (fn [acc add-id value-info]
                 (conj acc {:add-id add-id
                            :op-type :delete-value
                            :path op-path}))
               #{}
               current-add-id-to-value-info)))

(defn associative-get-delete-ops
  [{:keys [crdt get-child-path-info get-child-schema op-path path schema]
    :as arg}]
  (let [get-child-ops (fn [{:keys [k sub-path]}]
                        (get-delete-ops
                         (assoc arg
                                :crdt (get-in crdt [:children k])
                                :op-path (if (seq op-path)
                                           op-path
                                           [k])
                                :path sub-path
                                :schema (get-child-schema k))))]
    (if (empty? path)
      (reduce-kv (fn [acc k _]
                   (set/union acc (get-child-ops {:k k
                                                  :sub-path []})))
                 #{}
                 (:children crdt))
      (let [[k & ks] path]
        (get-child-ops (get-child-path-info path))))))

(defmethod get-delete-ops :map
  [{:keys [schema] :as arg}]
  (let [values-schema (l/schema-at-path schema ["x"])]
    (associative-get-delete-ops
     (assoc arg
            :get-child-path-info (fn [[k & sub-path]]
                                   (u/sym-map k sub-path))
            :get-child-schema (constantly values-schema)))))

(defmethod get-delete-ops :record
  [{:keys [schema] :as arg}]
  (associative-get-delete-ops
   (assoc arg
          :get-child-path-info (fn [[k & sub-path]]
                                 (u/sym-map k sub-path))
          :get-child-schema #(l/schema-at-path schema [%]))))

(defn get-ops-del-single-node
  [{:keys [crdt make-id node-id]}]
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
       :op-type :delete-array-edge}
      {:add-id out-add-id
       :op-type :delete-array-edge}
      {:add-id (make-id)
       :op-type :add-array-edge
       :value {:head-node-id (-> add-id-to-edge in-add-id :head-node-id)
               :tail-node-id (-> add-id-to-edge out-add-id :tail-node-id)}}}))

(defmethod get-delete-ops :array
  [{:keys [make-id crdt path schema] :as arg}]
  (let [items-schema (l/schema-at-path schema [0])
        info (array/get-linear-array-info arg)
        {:keys [crdt ordered-node-ids repair-ops]} info
        node-ops (associative-get-delete-ops
                  (assoc arg
                         :crdt crdt
                         :get-child-path-info (fn [[i & sub-path]]
                                                {:k (nth ordered-node-ids i)
                                                 :sub-path sub-path})
                         :get-child-schema (constantly items-schema)))
        {:keys [add-id-to-edge current-edge-add-ids]} crdt
        [i & sub-path] path
        edge-ops (cond
                   (empty? path)
                   (reduce (fn [acc eaid]
                             (conj acc {:add-id eaid
                                        :op-type :delete-array-edge}))
                           #{}
                           current-edge-add-ids)

                   (empty? sub-path)
                   (let [node-id (nth ordered-node-ids i)]
                     (get-ops-del-single-node (u/sym-map crdt make-id node-id)))

                   :else
                   #{})]
    (set/union repair-ops node-ops edge-ops)))

(defmethod get-delete-ops :union
  [{:keys [crdt op-path schema] :as arg}]
  (let [{:keys [union-branch]} crdt
        member-schema (when union-branch
                        (l/member-schema-at-branch schema union-branch))]
    (if member-schema
      (get-delete-ops (assoc arg
                             :op-path (conj op-path union-branch)
                             :schema member-schema))
      #{})))

(defmethod get-add-ops :single-value
  [{:keys [cmd-arg make-id op-path]}]
  #{{:add-id (make-id)
     :op-type :add-value
     :path op-path
     :value cmd-arg}})

(defmethod get-add-ops :map
  [{:keys [cmd-arg crdt op-path path schema] :as arg}]
  (let [values-schema (l/schema-at-path schema ["x"])]
    (if (empty? path)
      (do
        (when-not (associative? cmd-arg)
          (throw (ex-info
                  (str "Command path indicates a map, but `:arg` is "
                       "not associative. Got `" (or cmd-arg "nil") "`.")
                  {:arg cmd-arg
                   :op-path op-path
                   :path path})))
        (reduce-kv (fn [acc k v]
                     (set/union acc (get-add-ops
                                     (assoc arg
                                            :cmd-arg v
                                            :crdt (get-in crdt [:children k])
                                            :op-path [k]
                                            :schema values-schema))))
                   #{}
                   cmd-arg))
      (let [[k & ks] path]
        (get-add-ops (assoc arg
                            :crdt (get-in crdt [:children k])
                            :path ks
                            :schema values-schema))))))

(defmethod get-add-ops :record
  [{:keys [cmd-arg crdt op-path path schema] :as arg}]
  (if (empty? path)
    (do
      (when-not (associative? cmd-arg)
        (throw (ex-info
                (str "Command path indicates a record, but `:arg` is "
                     "not associative. Got `" (or cmd-arg "nil") "`.")
                {:arg cmd-arg
                 :op-path op-path
                 :path path})))
      (reduce (fn [acc k]
                (let [v (get cmd-arg k)
                      child-schema (l/schema-at-path schema [k])]
                  (set/union acc (get-add-ops
                                  (assoc arg
                                         :cmd-arg v
                                         :crdt (get-in crdt [:children k])
                                         :op-path [k]
                                         :schema child-schema)))))
              #{}
              (->> (l/edn schema)
                   (:fields)
                   (map :name))))
    (let [[k & ks] path]
      (get-add-ops (assoc arg
                          :crdt (get-in crdt [:children k])
                          :path ks
                          :schema (l/schema-at-path schema [k]))))))

(defmethod get-add-ops :union
  [{:keys [cmd-arg op-path schema] :as arg}]
  (let [ret (c/get-union-branch-and-schema-for-value {:schema schema
                                                      :v cmd-arg})
        {:keys [union-branch member-schema]} ret]
    (get-add-ops (assoc arg
                        :op-path (conj op-path union-branch)
                        :schema member-schema))))

(defmethod get-add-ops :array
  [{:keys [cmd-arg make-id op-path path schema] :as arg}]
  (let [items-schema (l/schema-at-path [0])
        info (array/get-linear-array-info arg)
        {:keys [crdt ordered-node-ids repair-ops]} info]
    (if (empty? path)
      (let [_ (when-not (sequential? cmd-arg)
                (throw (ex-info
                        (str "Command path indicates an array, but `:arg` is "
                             "not sequential. Got `" (or cmd-arg "nil") "`.")
                        {:arg cmd-arg
                         :op-path op-path
                         :path path})))
            info (reduce
                  (fn [acc v]
                    (let [node-id (make-id)
                          node-ops (get-add-ops
                                    (assoc arg
                                           :cmd-arg v
                                           :crdt (get-in crdt
                                                         [:children node-id])
                                           :op-path [node-id]
                                           :schema items-schema))]
                      (-> acc
                          (update :node-ops set/union node-ops)
                          (update :node-ids conj node-id))))
                  {:node-ids []
                   :node-ops #{}}
                  cmd-arg)
            {:keys [node-ids node-ops]} info
            initial-eops (if (empty? node-ids)
                           #{}
                           #{{:add-id (make-id)
                              :op-type :add-array-edge
                              :value {:head-node-id array/array-start-node-id
                                      :tail-node-id (first node-ids)}}
                             {:add-id (make-id)
                              :op-type :add-array-edge
                              :value {:head-node-id (last node-ids)
                                      :tail-node-id array/array-end-node-id}}})
            edge-ops (reduce (fn [acc [head-node-id tail-node-id]]
                               {:add-id (make-id)
                                :op-type :add-array-edge
                                :value (u/sym-map head-node-id tail-node-id)})
                             initial-eops
                             (partition 2 1 node-ids))]
        (set/union repair-ops node-ops edge-ops))
      (let [[i & ks] path
            k (nth ordered-node-ids i)
            child-ops (get-add-ops (assoc arg
                                          :crdt (get-in crdt [:children k])
                                          :path ks
                                          :schema items-schema))]
        (set/union repair-ops child-ops)))))

(defmethod process-cmd :set
  [arg]
  (let [del-ops (get-delete-ops arg)
        add-ops (get-add-ops arg)
        ops (set/union del-ops add-ops)]
    {:crdt (apply-ops/apply-ops (assoc arg :ops ops))
     :ops (set/union (:ops arg) ops)}))

(defmethod process-cmd :remove
  [arg]
  (let [ops (get-delete-ops arg)]
    {:crdt (apply-ops/apply-ops (assoc arg :ops ops))
     :ops (set/union (:ops arg) ops)}))

(defmethod process-cmd :insert-after
  [arg]
  (let [ops []]
    {:crdt (apply-ops/apply-ops (assoc arg :ops ops))
     :ops (set/union (:ops arg) ops)}))

(defn process-cmds
  [{:keys [cmds] :as arg}]
  (reduce (fn [{:keys [crdt ops]} cmd]
            (let [vpath (vec (:path cmd))]
              (process-cmd (assoc arg
                                  :cmd-arg (:arg cmd)
                                  :cmd-type (:op cmd)
                                  :crdt crdt
                                  :op-path vpath
                                  :ops ops
                                  :path vpath))))
          {:crdt (:crdt arg)
           :ops #{}}
          cmds))
