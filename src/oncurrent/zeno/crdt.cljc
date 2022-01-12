(ns oncurrent.zeno.crdt
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def array-end-node-id "-END-")
(def array-start-node-id "-START-")
(def container-types #{:array :map :record :union})
(def crdt-value-info-keys [:sys-time-ms :value])

(defn schema->dispatch-type [schema]
  (-> (l/schema-type schema)
      (container-types)
      (or :single-value)))

(defmulti apply-op (fn [{:keys [op-type schema]}]
                     [(schema->dispatch-type schema) op-type]))

(defmulti get-value (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmulti check-key (fn [{:keys [schema]}]
                      (schema->dispatch-type schema)))

(defmethod check-key :array
  [{:keys [add-id key op-type path]}]
  (when-not (string? key)
    (throw (ex-info (str "Array node-id key is not a string. Got: `"
                         (or key "nil") "`.")
                    (u/sym-map add-id key op-type path)))))

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

(defn same-cv-info? [x y]
  (and x
       y
       (= (select-keys x crdt-value-info-keys)
          (select-keys y crdt-value-info-keys))))

;; TODO: Replace this with conflict resolution?
(defn get-most-recent [current-add-id-to-value-info]
  (->> (vals current-add-id-to-value-info)
       (sort-by :sys-time-ms)
       (last)))

(defn get-single-value [{:keys [crdt schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt]
    (when (pos? (count current-add-id-to-value-info))
      (-> (get-most-recent current-add-id-to-value-info)
          (:value)))))

(defn get-array-edge-info
  [{:keys [add-id-to-edge
           current-edge-add-ids
           deleted-edge-add-ids]}]
  (let [get-edges (fn [add-ids]
                    (reduce
                     (fn [acc add-id]
                       (let [edge (-> (get add-id-to-edge add-id)
                                      (assoc :add-id add-id))]
                         (conj acc edge)))
                     #{}
                     add-ids))]
    (-> {}
        (assoc :edges (get-edges current-edge-add-ids))
        (assoc :deleted-edges (get-edges deleted-edge-add-ids)))))

(defn make-node->edge-info [edges]
  (reduce
   (fn [acc {:keys [head-node-id tail-node-id]}]
     (-> acc
         (update-in [head-node-id :children]
                    (fn [children]
                      (conj (or children #{})
                            tail-node-id)))
         (update-in [tail-node-id :parents]
                    (fn [parents]
                      (conj (or parents #{})
                            head-node-id)))))
   {}
   edges))

(defn connected-to-terminal?
  [{:keys [*nodes-connected-to-start
           *nodes-connected-to-end
           node
           node->edge-info
           path
           terminal]
    :as arg}]
  (let [[*nodes-connected link-key] (if (= :start terminal)
                                      [*nodes-connected-to-start :parents]
                                      [*nodes-connected-to-end :children])]
    (if (@*nodes-connected node)
      true
      (let [path* (conj path node)
            links (get-in node->edge-info [node link-key])]
        (reduce (fn [acc link]
                  (if-not (connected-to-terminal?
                           (assoc arg :node link :path path*))
                    acc
                    (do
                      (swap! *nodes-connected #(apply conj % path*))
                      (reduced true))))
                false
                links)))))

(defn get-connected-node
  [{:keys [*nodes-connected-to-start
           *nodes-connected-to-end
           deleted-edges
           node
           terminal] :as arg}]
  (let [node->deleted-edge-info (make-node->edge-info deleted-edges)
        [*nodes-connected
         links-k] (if (= :start terminal)
                    [*nodes-connected-to-start
                     :parents]
                    [*nodes-connected-to-end
                     :children])
        linked-nodes (-> (get-in node->deleted-edge-info [node links-k])
                         (sort))
        conn-node (reduce (fn [acc linked-node]
                            (when (@*nodes-connected linked-node)
                              (reduced linked-node)))
                          nil
                          linked-nodes)]
    (or conn-node
        (reduce (fn [acc linked-node]
                  (when-let [conn-node (get-connected-node
                                        (assoc arg :node linked-node))]
                    (reduced conn-node)))
                nil
                linked-nodes))))
(defn make-connecting-edges
  [{:keys [edges live-nodes make-add-id]
    :or {make-add-id u/compact-random-uuid}
    :as arg}]
  (-> (reduce (fn [acc terminal]
                (reduce
                 (fn [acc* node]
                   (let [node->edge-info (make-node->edge-info (:edges acc*))
                         arg* (assoc arg
                                     :node node
                                     :node->edge-info node->edge-info
                                     :terminal terminal)]
                     (if (connected-to-terminal? arg*)
                       acc*
                       (let [conn-node (get-connected-node arg*)
                             [self-add-id
                              opp-add-id] (if (= :start terminal)
                                            [:tail-node-id
                                             :head-node-id]
                                            [:head-node-id
                                             :tail-node-id])
                             edge {:add-id (make-add-id)
                                   self-add-id node
                                   opp-add-id conn-node}]
                         (-> acc*
                             (update :edges conj edge)
                             (update :new-edges conj edge))))))
                 acc
                 live-nodes))
              {:edges edges
               :new-edges #{}}
              [:start :end])
      :new-edges))

(defn get-node-connect-info
  [{:keys [edges deleted-edges live-nodes make-add-id]
    :or {make-add-id u/compact-random-uuid}}]
  (let [*nodes-connected-to-start (atom #{array-start-node-id})
        *nodes-connected-to-end (atom #{array-end-node-id})
        new-edges (make-connecting-edges (u/sym-map deleted-edges
                                                    edges
                                                    live-nodes
                                                    make-add-id
                                                    *nodes-connected-to-start
                                                    *nodes-connected-to-end))
        ops (map (fn [edge]
                   {:op-type :add-array-edge
                    :add-id (:add-id edge)
                    :value edge})
                 new-edges)]
    {:node-connect-ops ops
     :edges-after-node-connect (set/union edges new-edges)}))

(defn get-edge-cleanup-info
  [{:keys [deleted-edges edges live-nodes]}]
  (let [edge-info (reduce
                   (fn [acc edge]
                     (let [{:keys [add-id
                                   head-node-id
                                   tail-node-id]} edge]
                       (if (and (or (live-nodes head-node-id)
                                    (= array-start-node-id head-node-id))
                                (or (live-nodes tail-node-id)
                                    (= array-end-node-id tail-node-id)))
                         (update acc :connected-edges conj edge)
                         (update acc :disconnected-edges conj edge))))
                   {:disconnected-edges #{}
                    :connected-edges #{}}
                   edges)
        {:keys [connected-edges disconnected-edges]} edge-info]
    {:edge-cleanup-ops (map (fn [{:keys [add-id]}]
                              {:op-type :delete-array-edge
                               :add-id add-id})
                            disconnected-edges)
     :edges-after-cleanup connected-edges
     :deleted-edges-after-cleanup (set/union deleted-edges
                                             disconnected-edges)}))

(defn path-to-combining-node-info
  [{:keys [node->edge-info node]}]
  (when (and node (seq node->edge-info))
    (loop [cur-node node
           out []]
      (if (> (count (get-in node->edge-info [cur-node :parents])) 1)
        ;; We found the combining node
        {:path out
         :combining-node cur-node}
        ;; We can't have a splitting node when we're already in a split,
        ;; so this must be a simple linear node.
        (let [new-node (first (get-in node->edge-info [cur-node :children]))
              new-out (conj out cur-node)]
          (recur new-node new-out))))))

(defn find-edge
  [{:keys [edges head-node-id tail-node-id]}]
  (reduce (fn [acc edge]
            (when (and (= head-node-id (:head-node-id edge))
                       (= tail-node-id (:tail-node-id edge)))
              (reduced edge)))
          nil
          edges))

(defn get-conflict-resolution-op-infos
  [{:keys [combining-node
           edges
           make-add-id
           ops
           paths
           schema
           splitting-node]
    :or {make-add-id u/compact-random-uuid}}]
  (reduce (fn [acc [path next-path]]
            (let [edge-to-del-1 (find-edge {:edges (:edges acc)
                                            :head-node-id (last path)
                                            :tail-node-id combining-node})
                  del-op-1 {:add-id edge-to-del-1
                            :op-type :delete-array-edge}
                  edge-to-del-2 (find-edge
                                 {:edges (:edges acc)
                                  :head-node-id splitting-node
                                  :tail-node-id (first next-path)})
                  del-op-2 {:add-id edge-to-del-2
                            :op-type :delete-array-edge}
                  edge-to-add {:add-id (make-add-id)
                               :head-node-id (last path)
                               :tail-node-id (first next-path)}
                  add-op {:add-id (:add-id edge-to-add)
                          :op-type :add-array-edge
                          :value edge-to-add}]
              (-> acc
                  (update :edges #(-> %
                                      (disj edge-to-del-1)
                                      (disj edge-to-del-2)
                                      (conj edge-to-add)))
                  (update :ops #(-> %
                                    (conj del-op-1)
                                    (conj del-op-2)
                                    (conj add-op))))))
          (u/sym-map edges ops)
          (partition 2 1 paths)))

(defn get-conflict-resolution-info
  [{:keys [edges] :as arg}]
  (when (seq edges)
    (let [node->edge-info (make-node->edge-info edges)]
      (loop [node array-start-node-id
             acc {:edges edges
                  :ops []}]
        (if (= array-end-node-id node)
          acc
          (let [;; We sort by node add-id to guarantee consistent order
                children (sort (get-in node->edge-info [node :children]))]
            (if (= (count children) 1)
              (recur (first children) acc)
              ;; This is a splitting node
              (let [path-infos (map (fn [node*]
                                      (path-to-combining-node-info
                                       {:node node*
                                        :node->edge-info node->edge-info}))
                                    children)
                    {:keys [combining-node]} (first path-infos)
                    new-acc (get-conflict-resolution-op-infos
                             (assoc arg
                                    :combining-node combining-node
                                    :edges (:edges acc)
                                    :ops (:ops acc)
                                    :paths (map :path path-infos)
                                    :splitting-node node))]
                (recur combining-node new-acc)))))))))

(defn get-linear-array-info
  [{:keys [edges path]}]
  (when (seq edges)
    (let [node->edge-info (make-node->edge-info edges)]
      (when-not (node->edge-info array-start-node-id)
        (throw (ex-info (str "Array at path `" path "` does not have a start "
                             "node defined.")
                        (u/sym-map edges node->edge-info path))))
      (when-not (node->edge-info array-end-node-id)
        (throw (ex-info (str "Array at path `" path "` does not have an end "
                             "node defined.")
                        (u/sym-map edges node->edge-info path))))
      (loop [node-id array-start-node-id
             ordered-node-ids []]
        (let [children (get-in node->edge-info [node-id :children])
              child (first children)]
          (when-not child
            (throw (ex-info (str "Array at path `" path
                                 "` has a DAG is not connected.")
                            (u/sym-map edges node->edge-info path node-id))))
          (cond
            (> (count children) 1)
            {:linear? false}

            (= array-end-node-id child)
            {:linear? true
             :ordered-node-ids ordered-node-ids}

            :else
            (recur child (conj ordered-node-ids child))))))))

(defn get-non-linear-array-info [{:keys [crdt edges] :as arg}]
  (let [edge-cleanup-info (get-edge-cleanup-info arg)
        {:keys [deleted-edges-after-cleanup
                edge-cleanup-ops
                edges-after-cleanup]} edge-cleanup-info
        arg* (assoc arg
                    :edges edges-after-cleanup
                    :deleted-edges deleted-edges-after-cleanup)
        node-connect-info (get-node-connect-info arg*)
        {:keys [node-connect-ops
                edges-after-node-connect]} node-connect-info
        cr-info (get-conflict-resolution-info
                 (assoc arg :edges edges-after-node-connect))
        repair-ops (concat edge-cleanup-ops
                           node-connect-ops
                           (:ops cr-info))
        linear-edges (:edges cr-info)
        ret (get-linear-array-info (assoc arg :edges linear-edges))
        {:keys [linear? ordered-node-ids]} ret]
    (when-not linear?
      (throw (ex-info "Array CRDT DAG is not linear after repair ops."
                      (u/sym-map arg cr-info edge-cleanup-info
                                 node-connect-info repair-ops))))
    (u/sym-map ordered-node-ids repair-ops linear-edges)))

(defmethod apply-op [:single-value :add-value]
  [{:keys [add-id crdt op-type path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map add-id crdt path)))
    (let [{:keys [current-add-id-to-value-info deleted-add-ids]} crdt
          cur-value-info (get current-add-id-to-value-info add-id)
          value-info (select-keys arg crdt-value-info-keys)
          same? (same-cv-info? cur-value-info value-info)
          deleted? (get deleted-add-ids add-id)]
      (when (and value-info cur-value-info (not same?))
        (throw (ex-info
                (str "Attempt to reuse an existing add-id "
                     "(`" add-id "`) to add different value info to CRDT.")
                (u/sym-map add-id cur-value-info op-type value-info))))
      (if (or deleted? same?)
        crdt
        (assoc-in crdt [:current-add-id-to-value-info add-id] value-info)))))

(defmethod apply-op [:single-value :delete-value]
  [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-id-to-value-info dissoc add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defn associative-apply-op
  [{:keys [add-id get-child-schema crdt path schema value]
    :as arg}]
  (let [[k & ks] path]
    (check-key (assoc arg :key k))
    (update-in crdt [:children k]
               (fn [child-crdt]
                 (apply-op (assoc arg
                                  :crdt child-crdt
                                  :path ks
                                  :schema (get-child-schema k)))))))

(defn check-path-union-branch
  [{:keys [add-id op-type path schema value] :as arg}]
  (let [[union-branch & ks] path]
    (when-not (int? union-branch)
      (let [union-edn-schema (l/edn schema)]
        (throw (ex-info
                (str "Operating on a path that includes a union requires an "
                     "integer union-branch value in the path. Got `"
                     (or union-branch "nil") "`.")
                (u/sym-map add-id op-type path union-edn-schema value)))))))

(defmethod apply-op [:union :add-value]
  [{:keys [crdt path schema] :as arg}]
  (check-path-union-branch arg)
  (let [[union-branch & ks] path]
    (apply-op (assoc arg
                     :crdt (assoc crdt :union-branch union-branch)
                     :path ks
                     :schema (l/member-schema-at-branch schema union-branch)))))

(defn apply-union-op [{:keys [path schema] :as arg}]
  (check-path-union-branch arg)
  (let [[union-branch & ks] path
        member-schema (l/member-schema-at-branch schema union-branch)]
    (apply-op (assoc arg
                     :path ks
                     :schema member-schema))))

(defmethod apply-op [:union :delete-value]
  [arg]
  (apply-union-op arg))

(defmethod apply-op [:union :add-array-edge]
  [arg]
  (apply-union-op arg))

(defmethod apply-op [:union :delete-array-edge]
  [arg]
  (apply-union-op arg))

(defmethod apply-op [:map :add-value]
  [{:keys [add-id path schema value] :as arg}]
  (when (empty? path)
    (throw (ex-info (str "Path indicates a map, but no map key is given "
                         "in the path.")
                    (u/sym-map add-id path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defn associative-delete-value
  [{:keys [add-id get-child-schema crdt path] :as arg}]
  (associative-apply-op arg))

(defmethod apply-op [:map :delete-value]
  [{:keys [schema] :as arg}]
  (associative-delete-value (assoc arg :get-child-schema
                                   #(l/schema-at-path schema [%]))))

(defmethod apply-op [:map :add-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defmethod apply-op [:map :delete-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defmethod apply-op [:record :add-value]
  [{:keys [add-id path schema value] :as arg}]
  (when (empty? path)
    (throw (ex-info (str "Path indicates a record, but no record key is given "
                         "in the path.")
                    (u/sym-map add-id path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defmethod apply-op [:record :delete-value]
  [{:keys [schema] :as arg}]
  (associative-delete-value (assoc arg :get-child-schema
                                   #(l/schema-at-path schema [%]))))

(defmethod apply-op [:record :add-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defmethod apply-op [:record :delete-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (associative-apply-op (assoc arg :get-child-schema
                               #(l/schema-at-path schema [%]))))

(defmethod apply-op [:array :add-value]
  [{:keys [add-id path schema value] :as arg}]
  (when (empty? path)
    (throw (ex-info (str "Path indicates an array, but no array key is given "
                         "in the path.")
                    (u/sym-map add-id path value))))
  (associative-apply-op (assoc arg :get-child-schema
                               (fn [k]
                                 (l/schema-at-path schema [0])))))

(defmethod apply-op [:array :delete-value]
  [{:keys [schema] :as arg}]
  (let [get-child-schema (fn [k]
                           (l/schema-at-path schema [0]))]
    (associative-delete-value (assoc arg :get-child-schema get-child-schema))))

(defn check-edge [{:keys [add-id path] :as arg}]
  (let [edge (:value arg)]
    (doseq [k [:head-node-id :tail-node-id]]
      (let [v (get edge k)]
        (when (nil? v)
          (throw (ex-info (str "Missing " k " in edge.")
                          (u/sym-map add-id path edge))))
        (when-not (string? v)
          (throw (ex-info (str k " in edge is not a string. Got `" v "`.")
                          (u/sym-map add-id path edge))))))))

(defmethod apply-op [:array :add-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (when (seq path)
    (throw (ex-info "Can't index into array when adding an edge."
                    (u/sym-map path crdt add-id value))))
  ;; We use a slightly different CRDT implementation here because we
  ;; need to be able to resurrect deleted edges. The `:single-value`
  ;; CRDT does not keep info for deleted items.
  (let [{:keys [add-id-to-edge current-edge-add-ids deleted-edge-add-ids]} crdt
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
                                        #{add-id}))))))

(defmethod apply-op [:array :delete-array-edge]
  [{:keys [add-id crdt path schema value] :as arg}]
  (when (seq path)
    (throw (ex-info "Can't index into array when deleting an edge."
                    (u/sym-map path crdt add-id value))))
  ;; We use a slightly different CRDT implementation here because we
  ;; need to be able to resurrect deleted edges. The `:single-value`
  ;; CRDT does not keep info for deleted items.
  (-> crdt
      (update :current-edge-add-ids disj add-id)
      (update :deleted-edge-add-ids (fn [ids]
                                      (if (seq ids)
                                        (conj ids add-id)
                                        #{add-id})))))

(defn apply-ops [{:keys [crdt ops schema sys-time-ms]}]
  (reduce (fn [crdt* op]
            (apply-op (assoc op
                             :crdt crdt*
                             :schema schema
                             :sys-time-ms sys-time-ms)))
          crdt
          ops))

(defmethod get-value :single-value
  [{:keys [path] :as arg}]
  (if (seq path)
    (throw (ex-info "Can't index into a single-value CRDT." arg))
    (get-single-value arg)))

(defn associative-get-value
  [{:keys [get-child-schema crdt path] :as arg}]
  (if (empty? path)
    (reduce-kv (fn [acc k _]
                 (let [v (get-value (assoc arg
                                           :path [k]))]
                   (if (or (nil? v)
                           (and (coll? v) (empty? v)))
                     acc
                     (assoc acc k v))))
               {}
               (:children crdt))
    (let [[k & ks] path]
      ;; TODO: check that key is in the record
      (check-key (assoc arg :key k))
      (get-value (assoc arg
                        :crdt (get-in crdt [:children k])
                        :schema (get-child-schema k)
                        :path ks)))))

(defmethod get-value :map
  [{:keys [schema] :as arg}]
  (let [get-child-schema #(l/schema-at-path schema [%])]
    (associative-get-value (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value :record
  [{:keys [schema] :as arg}]
  (let [get-child-schema #(l/schema-at-path schema [%])]
    (associative-get-value (assoc arg :get-child-schema get-child-schema))))

(defmethod get-value :union
  [{:keys [crdt schema] :as arg}]
  (let [member-schema (l/member-schema-at-branch schema (:union-branch crdt))]
    (get-value (assoc arg :schema member-schema))))

(defmethod get-value :array
  [{:keys [crdt schema path] :as arg}]
  (let [arg* (assoc arg :get-child-schema (fn [k]
                                            (l/schema-at-path schema [0])))
        v (associative-get-value arg*)]
    (if (seq path)
      v
      (let [child-schema (l/schema-at-path schema [0])
            {:keys [nodes-crdt]} crdt
            {:keys [deleted-edges edges]} (get-array-edge-info crdt)
            live-nodes (reduce-kv
                        (fn [acc node-id crdt*]
                          (let [v* (get-value
                                    (assoc arg*
                                           :crdt crdt*
                                           :schema child-schema))]
                            (if (nil? v*)
                              acc
                              (conj acc node-id))))
                        #{}
                        (:children crdt))
            info-arg (u/sym-map crdt edges deleted-edges live-nodes path)
            ret (get-linear-array-info info-arg)
            {:keys [ordered-node-ids]} (if (:linear? ret)
                                         ret
                                         (get-non-linear-array-info info-arg))]
        (mapv v ordered-node-ids)))))
