(ns com.oncurrent.zeno.state-providers.crdt.array
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as aoi]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def array-end-node-id "-END-")
(def array-start-node-id "-START-")

(declare repair-array)

(defn get-edges [{:keys [crdt edge-type]}]
  (let [{:keys [add-id-to-edge]} crdt
        add-ids (case edge-type
                  nil (:current-edge-add-ids crdt)
                  :current (:current-edge-add-ids crdt)
                  :deleted (:deleted-edge-add-ids crdt))]
    (reduce (fn [acc add-id]
              (let [edge (-> (get add-id-to-edge add-id)
                             (assoc :add-id add-id))]
                (conj acc edge)))
            #{}
            add-ids)))

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
  [{:keys [nodes-connected-to-start
           nodes-connected-to-end
           node
           node->edge-info
           path
           terminal]
    :as arg}]
  (let [[nodes-connected link-key] (if (= :start terminal)
                                     [nodes-connected-to-start :parents]
                                     [nodes-connected-to-end :children])]
    (if (nodes-connected node)
      true
      (let [path* (conj path node)
            links (get-in node->edge-info [node link-key])]
        (reduce (fn [acc link]
                  (if-not (connected-to-terminal?
                           (assoc arg :node link :path path*))
                    acc
                    (reduced true)))
                false
                links)))))

(defn get-connected-node
  [{:keys [nodes-connected-to-start
           nodes-connected-to-end
           deleted-edges
           node
           terminal] :as arg}]
  (let [node->deleted-edge-info (make-node->edge-info deleted-edges)
        [nodes-connected
         links-k] (if (= :start terminal)
                    [nodes-connected-to-start
                     :parents]
                    [nodes-connected-to-end
                     :children])
        linked-nodes (-> (get-in node->deleted-edge-info [node links-k])
                         (sort))
        conn-node (reduce (fn [acc linked-node]
                            (when (nodes-connected linked-node)
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

(defn make-connections-to-terminal
  [{:keys [make-id terminal] :as arg}]
  (let [make-id (or make-id u/compact-random-uuid)]
    (reduce (fn [acc node]
              (let [node->edge-info (make-node->edge-info (:edges acc))
                    arg* (assoc arg
                                :node node
                                :node->edge-info node->edge-info
                                :terminal terminal)]
                (if (connected-to-terminal? arg*)
                  acc
                  (let [conn-node (get-connected-node arg*)
                        [self-add-id opp-add-id] (if (= :start terminal)
                                                   [:tail-node-id
                                                    :head-node-id]
                                                   [:head-node-id
                                                    :tail-node-id])
                        edge {:add-id (make-id)
                              self-add-id node
                              opp-add-id conn-node}
                        new-edges (:new-edges acc)]
                    (-> acc
                        (update :edges conj edge)
                        (update :new-edges conj edge))))))
            arg
            (:live-nodes arg))))

(defn make-connecting-edges [{:keys [edges] :as arg}]
  (if (empty? edges)
    #{}
    (-> (reduce (fn [acc terminal]
                  (make-connections-to-terminal
                   (merge arg acc (u/sym-map terminal))))
                {:edges edges
                 :new-edges #{}}
                [:start :end])
        (:new-edges))))

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

(defn find-edge-add-id
  [{:keys [edges head-node-id tail-node-id] :as arg}]
  (when (empty? edges)
    (throw (ex-info "No edges provided to `find-edge-add-id`." arg)))
  (reduce (fn [acc edge]
            (when (and (= head-node-id (:head-node-id edge))
                       (= tail-node-id (:tail-node-id edge)))
              (reduced (:add-id edge))))
          nil
          edges))

(defn get-serializing-crdt-ops-for-path
  [{:keys [combining-node edges make-id paths splitting-node sys-time-ms]}]
  (let [make-id (or make-id u/compact-random-uuid)]
    (reduce (fn [acc [path next-path]]
              (let [del-op-1 {:add-id (find-edge-add-id
                                       {:edges edges
                                        :head-node-id (last path)
                                        :tail-node-id combining-node})
                              :op-path '()
                              :op-type :delete-array-edge}
                    del-op-2 {:add-id (find-edge-add-id
                                       {:edges edges
                                        :head-node-id splitting-node
                                        :tail-node-id (first next-path)})
                              :op-path '()
                              :op-type :delete-array-edge}
                    add-id (make-id)
                    add-op {:add-id add-id
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id (last path)
                                    :tail-node-id (first next-path)}}]
                (-> acc
                    (conj del-op-1)
                    (conj del-op-2)
                    (conj add-op))))
            #{}
            (partition 2 1 paths))))

(defn get-serializing-crdt-ops [{:keys [edges] :as arg}]
  (let [node->edge-info (make-node->edge-info edges)]
    (loop [node array-start-node-id
           crdt-ops #{}]
      (if (= array-end-node-id node)
        crdt-ops
        (let [;; We sort by node add-id to guarantee consistent order
              children (sort (get-in node->edge-info [node :children]))]
          (if (= (count children) 1)
            (recur (first children) crdt-ops)
            ;; This is a splitting node
            (let [path-infos (map (fn [node*]
                                    (path-to-combining-node-info
                                     {:node node*
                                      :node->edge-info node->edge-info}))
                                  children)
                  {:keys [combining-node]} (first path-infos)]
              (recur combining-node
                     (set/union crdt-ops
                                (get-serializing-crdt-ops-for-path
                                 (assoc arg
                                        :combining-node combining-node
                                        :paths (map :path path-infos)
                                        :splitting-node node)))))))))))

(defn get-array-info
  [{:keys [crdt path] :as arg}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})]
    (if (empty? edges)
      {:linear? true
       :ordered-node-ids []}
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
                                   "` has a DAG that is not connected.")
                              (u/sym-map edges node->edge-info path node-id))))
            (cond
              (> (count children) 1)
              {:linear? false}

              (= array-end-node-id child)
              {:linear? true
               :ordered-node-ids ordered-node-ids}

              :else
              (recur child (conj ordered-node-ids child)))))))))

(defn get-live-nodes [{:keys [crdt schema] :as arg}]
  (let [child-schema (l/child-schema schema)]
    (reduce-kv
     (fn [acc node-id child-crdt]
       (if (-> (assoc arg
                      :crdt child-crdt
                      :path []
                      :schema child-schema)
               (c/get-value-info)
               (:value)
               (nil?))
         acc
         (conj acc node-id)))
     #{}
     (:children crdt))))

(defn delete-dangling-edges [{:keys [live-nodes] :as arg}]
  (let [crdt-ops (reduce
                  (fn [acc edge]
                    (let [{:keys [add-id
                                  head-node-id
                                  tail-node-id]} edge]
                      (if (and (or (live-nodes head-node-id)
                                   (= array-start-node-id head-node-id))
                               (or (live-nodes tail-node-id)
                                   (= array-end-node-id tail-node-id)))
                        acc
                        (conj acc {:add-id add-id
                                   :op-path '()
                                   :op-type :delete-array-edge}))))
                  #{}
                  (get-edges arg))
        crdt* (aoi/apply-ops-without-repair (assoc arg :crdt-ops crdt-ops))]
    (-> arg
        (assoc :crdt crdt*)
        (update :repair-crdt-ops set/union crdt-ops))))

(defn get-nodes-connected-to-terminals [edges]
  (reduce (fn [acc {:keys [head-node-id tail-node-id]}]
            (cond
              (= array-start-node-id head-node-id)
              (update acc :nodes-connected-to-start conj tail-node-id)

              (= array-end-node-id tail-node-id)
              (update acc :nodes-connected-to-end conj head-node-id)

              :else acc))
          {:nodes-connected-to-start #{array-start-node-id}
           :nodes-connected-to-end #{array-end-node-id}}
          edges))

(defn connect-nodes-to-terminals
  [{:keys [live-nodes make-id sys-time-ms] :as arg}]
  (let [edges (get-edges (assoc arg :edge-type :current))
        deleted-edges (get-edges (assoc arg :edge-type :deleted))
        {:keys [nodes-connected-to-start
                nodes-connected-to-end]} (get-nodes-connected-to-terminals edges)
        new-edges (make-connecting-edges (u/sym-map deleted-edges
                                                    edges
                                                    live-nodes
                                                    make-id
                                                    nodes-connected-to-start
                                                    nodes-connected-to-end))
        crdt-ops (map (fn [edge]
                        {:add-id (:add-id edge)
                         :op-path '()
                         :op-type :add-array-edge
                         :sys-time-ms (or sys-time-ms (u/current-time-ms))
                         :value (select-keys edge
                                             [:head-node-id :tail-node-id])})
                      new-edges)
        crdt* (aoi/apply-ops-without-repair (assoc arg :crdt-ops crdt-ops))]
    (-> arg
        (assoc :crdt crdt*)
        (update :repair-crdt-ops set/union crdt-ops))))

(defn serialize-parallel-paths [{:keys [crdt] :as arg}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})]
    (if (empty? edges)
      arg
      (let [crdt-ops (get-serializing-crdt-ops (assoc arg :edges edges))
            crdt* (aoi/apply-ops-without-repair (assoc arg :crdt-ops crdt-ops))]
        (-> arg
            (assoc :crdt crdt*)
            (update :repair-crdt-ops set/union crdt-ops))))))

(defn update-ordered-node-ids [{:keys [crdt] :as arg}]
  (let [{:keys [linear? ordered-node-ids]} (get-array-info arg)]
    (when-not linear?
      (throw (ex-info "Array is not linear after repair."
                      (u/sym-map crdt))))
    (assoc-in arg [:crdt :ordered-node-ids] ordered-node-ids)))

(defn repair-array* [{:keys [schema] :as arg}]
  (-> (assoc arg
             :live-nodes (get-live-nodes arg)
             :sys-time-ms (u/current-time-ms))
      (delete-dangling-edges)
      (connect-nodes-to-terminals)
      (serialize-parallel-paths)
      (select-keys [:crdt :repair-crdt-ops])))

(defmethod c/repair :array
  [{:keys [crdt schema] :as arg}]
  (let [items-schema (l/child-schema schema)
        info (get-array-info arg)
        repaired (cond
                   (not (:linear? info))
                   (repair-array* arg)

                   (empty? (:ordered-node-ids crdt))
                   (-> {:crdt crdt
                        :repair-crdt-ops #{}}
                       (assoc-in [:crdt :ordered-node-ids]
                                 (:ordered-node-ids info)))

                   :else
                   {:crdt crdt
                    :repair-crdt-ops #{}})
        updated (update-ordered-node-ids repaired)]
    (reduce
     (fn [acc node-id]
       (let [ret (c/repair (assoc arg
                                  :crdt (get-in updated
                                                [:crdt :children node-id])
                                  :schema items-schema))
             ops (c/xf-op-paths {:prefix node-id
                                 :crdt-ops (:repair-crdt-ops ret)})]
         (-> acc
             (assoc-in [:crdt :children node-id] (:crdt ret))
             (update :repair-crdt-ops set/union ops))))
     updated
     (-> updated :crdt :ordered-node-ids))))

(defn get-sub-value-info
  [{:keys [get-child-schema crdt norm-path path] :as arg}]
  (if (empty? path)
    (let [value (reduce-kv
                 (fn [acc k _]
                   (let [v (-> (assoc arg :path [k])
                               (c/get-value-info)
                               (:value))]
                     (if (= c/not-present v)
                       acc
                       (assoc acc k v))))
                 {}
                 (:children crdt))]
      (u/sym-map value norm-path))
    (let [[k & ks] path]
      (if-not k
        {:norm-path norm-path
         :value nil}
        (do
          (c/check-key (assoc arg :key k))
          (c/get-value-info (assoc arg
                                   :crdt (get-in crdt [:children k])
                                   :path ks
                                   :schema (get-child-schema k))))))))

(defmethod c/get-value-info :array
  [{:keys [crdt norm-path path schema] :as arg}]
  (let [{:keys [container-add-ids]} crdt
        ordered-node-ids (or (:ordered-node-ids crdt) [])
        arg* (assoc arg :get-child-schema (fn [_]
                                            (l/child-schema schema)))
        exists? (seq container-add-ids)]
    (if (empty? path)
      (let [id->v (when exists?
                    (:value (get-sub-value-info arg*)))
            v (if exists?
                (mapv id->v ordered-node-ids)
                c/not-present)]
        {:norm-path norm-path
         :value v})
      (let [[raw-k & sub-path] path
            [k i] (cond
                    (string? raw-k)
                    [raw-k nil]

                    (integer? raw-k)
                    (let [array-len (count ordered-node-ids)
                          ni (u/get-normalized-array-index {:array-len array-len
                                                            :i raw-k})]
                      (when (or (not ni) (empty? ordered-node-ids))
                        (throw
                         (ex-info
                          (str "Index `" ni "` into array `" ordered-node-ids
                               "` is out of bounds.")
                          (u/sym-map norm-path path ordered-node-ids
                                     raw-k sub-path))))
                      [(nth ordered-node-ids ni) ni])

                    :else
                    (throw (ex-info (str "Unknown key type in :array path `"
                                         raw-k "`.")
                                    {:k raw-k
                                     :path path})))
            new-path (vec (cons k (rest path)))]
        (get-sub-value-info (assoc arg*
                                   :path new-path
                                   :norm-path (conj (or norm-path []) i)
                                   :string-array-keys? true))))))

(defmulti get-edge-insert-info :cmd-type)

(defmethod get-edge-insert-info :zeno/insert-before
  [{:keys [crdt i make-id new-node-id ordered-node-ids sys-time-ms]}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})
        node-id (or (get ordered-node-ids i)
                    array-end-node-id)
        prev-node-id (if (pos? i)
                       (get ordered-node-ids (dec i))
                       array-start-node-id)
        edge-to-rm (reduce (fn [acc edge]
                             (when (and (= prev-node-id (:head-node-id edge))
                                        (= node-id (:tail-node-id edge)))
                               (reduced (:add-id edge))))
                           nil
                           edges)
        crdt-ops (cond-> #{{:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id prev-node-id
                                    :tail-node-id new-node-id}}
                           {:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id new-node-id
                                    :tail-node-id node-id}}}
                   edge-to-rm (conj {:add-id edge-to-rm
                                     :op-path '()
                                     :op-type :delete-array-edge}))
        [head tail] (split-at i ordered-node-ids)]
    {:crdt-ops crdt-ops
     :ordered-node-ids (vec (concat head [new-node-id] tail))}))

(defmethod get-edge-insert-info :zeno/insert-after
  [{:keys [crdt i make-id new-node-id ordered-node-ids sys-time-ms]}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})
        node-id (or (get ordered-node-ids i)
                    array-start-node-id)
        next-node-id (if (< (inc i) (count ordered-node-ids))
                       (get ordered-node-ids (inc i))
                       array-end-node-id)
        edge-to-rm (reduce (fn [acc edge]
                             (when (and (= node-id (:head-node-id edge))
                                        (= next-node-id (:tail-node-id edge)))
                               (reduced (:add-id edge))))
                           nil
                           edges)
        add-id-1 (make-id)
        add-id-2 (make-id)
        crdt-ops (cond-> #{{:add-id add-id-1
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id node-id
                                    :tail-node-id new-node-id}}
                           {:add-id add-id-2
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id new-node-id
                                    :tail-node-id next-node-id}}}
                   edge-to-rm (conj {:add-id edge-to-rm
                                     :op-path '()
                                     :op-type :delete-array-edge}))
        [head tail] (split-at (inc i) ordered-node-ids)]
    {:crdt-ops crdt-ops
     :ordered-node-ids (vec (concat head [new-node-id] tail))}))

(defmethod get-edge-insert-info :zeno/insert-range-after
  [{:keys [crdt i make-id new-node-ids ordered-node-ids sys-time-ms]}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})
        node-id (or (get ordered-node-ids i)
                    array-start-node-id)
        next-node-id (if (< (inc i) (count ordered-node-ids))
                       (get ordered-node-ids (inc i))
                       array-end-node-id)
        edge-to-rm (reduce (fn [acc edge]
                             (when (and (= node-id (:head-node-id edge))
                                        (= next-node-id (:tail-node-id edge)))
                               (reduced (:add-id edge))))
                           nil
                           edges)
        initial-crdt-ops #{{:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id node-id
                                    :tail-node-id (first new-node-ids)}}
                           {:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id (last new-node-ids)
                                    :tail-node-id next-node-id}}}
        add-crdt-ops (reduce (fn [acc [head-node-id tail-node-id]]
                               (conj acc
                                     {:add-id (make-id)
                                      :op-path '()
                                      :op-type :add-array-edge
                                      :sys-time-ms (or sys-time-ms
                                                       (u/current-time-ms))
                                      :value (u/sym-map head-node-id
                                                        tail-node-id)}))
                             initial-crdt-ops
                             (partition 2 1 new-node-ids))
        crdt-ops (cond-> add-crdt-ops
                   edge-to-rm (conj {:add-id edge-to-rm
                                     :op-path '()
                                     :op-type :delete-array-edge}))
        [head tail] (split-at (inc i) ordered-node-ids)]
    {:crdt-ops crdt-ops
     :ordered-node-ids (vec (concat head new-node-ids tail))}))

(defmethod get-edge-insert-info :zeno/insert-range-before
  [{:keys [crdt i make-id new-node-ids ordered-node-ids sys-time-ms]}]
  (let [edges (get-edges {:crdt crdt
                          :edge-type :current})
        node-id (or (get ordered-node-ids i)
                    array-end-node-id)
        prev-node-id (if (pos? i)
                       (get ordered-node-ids (dec i))
                       array-start-node-id)
        edge-to-rm (reduce (fn [acc edge]
                             (when (and (= prev-node-id (:head-node-id edge))
                                        (= node-id (:tail-node-id edge)))
                               (reduced (:add-id edge))))
                           nil
                           edges)
        initial-crdt-ops #{{:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id prev-node-id
                                    :tail-node-id (first new-node-ids)}}
                           {:add-id (make-id)
                            :op-path '()
                            :op-type :add-array-edge
                            :sys-time-ms (or sys-time-ms (u/current-time-ms))
                            :value {:head-node-id (last new-node-ids)
                                    :tail-node-id node-id}}}
        add-crdt-ops (reduce (fn [acc [head-node-id tail-node-id]]
                               (conj acc
                                     {:add-id (make-id)
                                      :op-path '()
                                      :op-type :add-array-edge
                                      :sys-time-ms (or sys-time-ms
                                                       (u/current-time-ms))
                                      :value (u/sym-map head-node-id
                                                        tail-node-id)}))
                             initial-crdt-ops
                             (partition 2 1 new-node-ids))
        crdt-ops (cond-> add-crdt-ops
                   edge-to-rm (conj {:add-id edge-to-rm
                                     :op-path '()
                                     :op-type :delete-array-edge}))
        [head tail] (split-at i ordered-node-ids)]
    {:crdt-ops crdt-ops
     :ordered-node-ids (vec (concat head new-node-ids tail))}))
