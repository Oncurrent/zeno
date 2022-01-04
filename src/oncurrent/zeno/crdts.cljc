(ns oncurrent.zeno.crdts
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:map :array :record})
(def array-end-add-id "-END-")
(def array-start-add-id "-START-")

(defmulti apply-op #(-> % :op :op-type))

(defmulti get-crdt-val
  (fn [{:keys [item-id k schema] :as arg}]
    (let [schema-type (l/schema-type schema)]
      (cond
        (and k (= :map schema-type)) :map-kv
        (= :map schema-type) :map
        (and k (= :record schema-type)) :record-kv
        (= :record schema-type) :record
        (and k (= :array schema-type)) :array-kv
        (= :array schema-type) :array
        :else :single-value))))

(def crdt-value-info-keys [:add-id :sys-time-ms :union-branch :value])

(defn same-cv-info? [x y]
  (and x
       y
       (= (select-keys x crdt-value-info-keys)
          (select-keys y crdt-value-info-keys))))

(defn add-to-crdt
  [{:keys [add-id crdt value-info] :as arg}]
  (let [{:keys [current-add-id-to-value-info deleted-add-ids]} crdt
        cur-value-info (get current-add-id-to-value-info add-id)
        same? (same-cv-info? value-info cur-value-info)
        deleted? (get deleted-add-ids add-id)]
    (when (and value-info cur-value-info (not same?))
      (throw (ex-info
              (str "Attempt to reuse an existing add-id "
                   "(`" add-id "`) to add different value info to CRDT.")
              (u/sym-map add-id cur-value-info value-info))))
    (if (or deleted? same?)
      crdt
      (update crdt :current-add-id-to-value-info assoc add-id value-info))))

(defn del-from-crdt [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-id-to-value-info dissoc add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defn add-to-vhs-crdt
  [{:keys [add-id crdt value-info] :as arg}]
  (let [{:keys [add-id-to-value-info current-add-ids deleted-add-ids]} crdt
        deleted? (get deleted-add-ids add-id)
        current? (get current-add-ids add-id)
        info (get add-id-to-value-info add-id)
        same? (same-cv-info? value-info crdt)]
    (when (and info crdt (not same?))
      (throw (ex-info
              (str "Attempt to reuse an existing add-id "
                   "(`" add-id "`) to add different info to CRDT.")
              arg)))
    (cond-> (assoc-in crdt [:add-id-to-value-info add-id] value-info)
      (not deleted?)
      (update :current-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id}))))))

(defn del-from-vhs-crdt [{:keys [add-id crdt]}]
  (-> crdt
      (update :current-add-ids disj add-id)
      (update :deleted-add-ids (fn [ids]
                                 (if (seq ids)
                                   (conj ids add-id)
                                   #{add-id})))))

(defmethod apply-op :add-single-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op
        value-info (select-keys op crdt-value-info-keys)]
    (update-in data-store [:single-value-crdts item-id]
               (fn [crdt]
                 (add-to-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :del-single-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op]
    (update-in data-store [:single-value-crdts item-id]
               (fn [crdt]
                 (del-from-crdt (u/sym-map add-id crdt))))))

(defmethod apply-op :add-record-key-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id k]} op
        value-info (select-keys op crdt-value-info-keys)]
    (update-in data-store [:record-kv-crdts item-id k]
               (fn [crdt]
                 (add-to-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :del-record-key-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id k]} op]
    (update-in data-store  [:record-kv-crdts item-id k]
               (fn [crdt]
                 (del-from-crdt (u/sym-map add-id crdt))))))

(defmethod apply-op :add-map-key-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id k]} op
        value-info (select-keys op crdt-value-info-keys)]
    (update-in data-store [:map-kv-crdts item-id k]
               (fn [crdt]
                 (add-to-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :del-map-key-value
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id k]} op]
    (update-in data-store [:map-kv-crdts item-id k]
               (fn [crdt]
                 (del-from-crdt (u/sym-map add-id crdt))))))

(defmethod apply-op :add-map-key
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id k]} op
        value-info (-> (select-keys op crdt-value-info-keys)
                       (assoc :value k))]
    (update-in data-store [:map-keyset-crdts item-id]
               (fn [crdt]
                 (add-to-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :del-map-key
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op]
    (update-in data-store [:map-keyset-crdts item-id]
               (fn [crdt]
                 (del-from-crdt (u/sym-map add-id crdt))))))

(defmethod apply-op :add-array-node
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op
        value-info (select-keys op crdt-value-info-keys)]
    (update-in data-store [:array-nodes-crdts item-id]
               (fn [crdt]
                 (add-to-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :add-array-edge
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op
        value-info (select-keys op crdt-value-info-keys)]
    (update-in data-store [:array-edges-crdts item-id]
               (fn [crdt]
                 (add-to-vhs-crdt (u/sym-map add-id crdt value-info))))))

(defmethod apply-op :del-array-node
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op]
    (update-in data-store [:array-nodes-crdts item-id]
               (fn [crdt]
                 (del-from-crdt (u/sym-map add-id crdt))))))

(defmethod apply-op :del-array-edge
  [{:keys [data-store op]}]
  (let [{:keys [add-id item-id]} op]
    (update-in data-store [:array-edges-crdts item-id]
               (fn [crdt]
                 (del-from-vhs-crdt (u/sym-map add-id crdt))))))

(defn apply-ops
  [{:keys [data-store ops]}]
  (reduce (fn [data-store* op]
            (apply-op {:data-store data-store* :op op}))
          data-store
          ops))

(defn get-most-recent [current-add-id-to-value-info]
  (->> (vals current-add-id-to-value-info)
       (sort-by :sys-time-ms)
       (last)))

(defn get-single-value-crdt-val
  [{:keys [crdt item-id schema] :as arg}]
  (let [{:keys [current-add-id-to-value-info]} crdt
        num-values (count current-add-id-to-value-info)]
    (when (pos? num-values)
      (let [schema-type (l/schema-type schema)
            value-info (get-most-recent current-add-id-to-value-info)
            {:keys [value union-branch]} value-info]
        (cond
          (and (= :union schema-type) union-branch)
          (let [member-schema (-> (l/edn schema)
                                  (nth union-branch)
                                  (l/edn->schema))
                member-schema-type (l/schema-type member-schema)]
            (if-not (container-types member-schema-type)
              value
              (get-crdt-val (-> arg
                                (assoc :schema member-schema)
                                (assoc :item-id value)))))

          (container-types schema-type)
          (get-crdt-val (assoc arg :item-id value))

          :else
          value)))))

(defmethod get-crdt-val :map-kv
  [{:keys [data-store item-id k schema] :as arg}]
  (let [crdt (get-in data-store [:map-kv-crdts item-id k])
        values-schema (l/schema-at-path schema [k])]
    (get-single-value-crdt-val (-> arg
                                   (assoc :crdt crdt)
                                   (assoc :schema values-schema)
                                   (dissoc :k)))))

(defmethod get-crdt-val :record-kv
  [{:keys [data-store item-id k schema] :as arg}]
  (let [crdt (get-in data-store [:record-kv-crdts item-id k])
        field-schema (l/schema-at-path schema [k])]
    (get-single-value-crdt-val (-> arg
                                   (assoc :crdt crdt)
                                   (assoc :schema field-schema)
                                   (dissoc :k)))))

(defmethod get-crdt-val :map
  [{:keys [data-store item-id schema] :as arg}]
  (let [keyset-crdt (get-in data-store [:map-keyset-crdts item-id])
        infos (vals (:current-add-id-to-value-info keyset-crdt))]
    (reduce (fn [acc info]
              (let [k (:value info)
                    v (get-crdt-val (assoc arg
                                           :k k))]
                (assoc acc k v)))
            {}
            infos)))

(defmethod get-crdt-val :record
  [{:keys [item-id schema] :as arg}]
  (let [field-ks (->> (l/edn schema)
                      (:fields)
                      (map :name))]
    (reduce (fn [acc k]
              (let [v (get-crdt-val (assoc arg :k k))]
                (cond-> acc
                  (not (nil? v)) (assoc k v))))
            {}
            field-ks)))

(defn make-node->edge-info [edges]
  (reduce
   (fn [acc {:keys [head-node-add-id tail-node-add-id]}]
     (-> acc
         (update-in [head-node-add-id :children]
                    (fn [children]
                      (conj (or children #{})
                            tail-node-add-id)))
         (update-in [tail-node-add-id :parents]
                    (fn [parents]
                      (conj (or parents #{})
                            head-node-add-id)))))
   {}
   edges))

(defn get-array-edge-info
  [{:keys [data-store item-id]}]
  (let [crdt (get-in data-store [:array-edges-crdts item-id])
        {:keys [add-id-to-value-info current-add-ids deleted-add-ids]} crdt
        get-edges (fn [add-ids]
                    (reduce
                     (fn [acc add-id]
                       (let [edge (-> (get add-id-to-value-info add-id)
                                      (:value)
                                      (assoc :add-id add-id))]
                         (conj acc edge)))
                     #{}
                     add-ids))]
    (-> {}
        (assoc :edges (get-edges current-add-ids))
        (assoc :deleted-edges (get-edges deleted-add-ids)))))

(defn get-array-node-add-id->item-id
[{:keys [data-store item-id schema] :as arg}]
(let [crdt (get-in data-store [:array-nodes-crdts item-id])
      {:keys [current-add-id-to-value-info]} crdt]
  (reduce-kv (fn [acc add-id value-info]
               (let [item-id (:value value-info)]
                 (assoc acc add-id item-id)))
             {}
             current-add-id-to-value-info)))

(defn get-linear-array-info
[{:keys [edges item-id] :as arg}]
(when (seq edges)
  (let [node->edge-info (make-node->edge-info edges)]
    (when-not (node->edge-info array-start-add-id)
      (throw (ex-info (str "Array `" item-id "` does not have a start node "
                           "defined.")
                      (u/sym-map edges node->edge-info item-id))))
    (when-not (node->edge-info array-end-add-id)
      (throw (ex-info (str "Array `" item-id "` does not have an end node "
                           "defined.")
                      (u/sym-map edges node->edge-info item-id))))
    (loop [node-id array-start-add-id
           node-add-ids []]
      (let [children (get-in node->edge-info [node-id :children])
            child (first children)]
        (when-not child
          (throw (ex-info (str "Array `" item-id
                               "` has a DAG is not connected.")
                          (u/sym-map edges node->edge-info item-id
                                     node-id))))
        (cond
          (> (count children) 1)
          {:linear? false}

          (= array-end-add-id child)
          {:linear? true
           :node-add-ids node-add-ids}

          :else
          (recur child (conj node-add-ids child))))))))

(defn get-edge-cleanup-info
  [{:keys [deleted-edges edges item-id live-nodes]}]
  (let [edge-info (reduce
                   (fn [acc edge]
                     (let [{:keys [add-id
                                   head-node-add-id
                                   tail-node-add-id]} edge]
                       (if (and (or (live-nodes head-node-add-id)
                                    (= array-start-add-id head-node-add-id))
                                (or (live-nodes tail-node-add-id)
                                    (= array-end-add-id tail-node-add-id)))
                         (update acc :connected-edges conj edge)
                         (update acc :disconnected-edges conj edge))))
                   {:disconnected-edges #{}
                    :connected-edges #{}}
                   edges)
        op-type :del-array-edge
        base-op (u/sym-map item-id op-type)
        {:keys [connected-edges disconnected-edges]} edge-info]
    {:edge-cleanup-ops (map #(assoc base-op :add-id (:add-id %))
                            disconnected-edges)
     :edges-after-cleanup connected-edges
     :deleted-edges-after-cleanup (set/union deleted-edges
                                             disconnected-edges)}))

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
                                            [:tail-node-add-id
                                             :head-node-add-id]
                                            [:head-node-add-id
                                             :tail-node-add-id])
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
  [{:keys [edges deleted-edges item-id live-nodes make-add-id]
    :or {make-add-id u/compact-random-uuid}}]
  (let [op-type :add-array-edge
        base-op (u/sym-map item-id op-type)
        *nodes-connected-to-start (atom #{array-start-add-id})
        *nodes-connected-to-end (atom #{array-end-add-id})
        new-edges (make-connecting-edges (u/sym-map deleted-edges
                                                    edges
                                                    live-nodes
                                                    make-add-id
                                                    *nodes-connected-to-start
                                                    *nodes-connected-to-end))
        op-type :add-array-edge
        base-op (u/sym-map item-id op-type)
        ops (map (fn [edge]
                   (assoc base-op
                          :add-id (:add-id edge)
                          :value edge))
                 new-edges)]
    {:node-connect-ops ops
     :edges-after-node-connect (set/union edges new-edges)}))

(defn path-to-combining-node-info
  [{:keys [node->edge-info node]}]
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
        (recur new-node new-out)))))

(defn find-edge
  [{:keys [edges head-node-add-id tail-node-add-id]}]
  (reduce (fn [acc edge]
            (when (and (= head-node-add-id (:head-node-add-id edge))
                       (= tail-node-add-id (:tail-node-add-id edge)))
              (reduced edge)))
          nil
          edges))

(defn get-conflict-resolution-op-infos
  [{:keys [combining-node
           edges
           item-id
           make-add-id
           ops
           paths
           schema
           splitting-node]
    :or {make-add-id u/compact-random-uuid}}]
  (let [base-op (u/sym-map item-id schema)]
    (reduce (fn [acc [path next-path]]
              (let [edge-to-del-1 (find-edge {:edges (:edges acc)
                                              :head-node-add-id (last path)
                                              :tail-node-add-id combining-node})
                    del-op-1 (assoc base-op
                                    :add-id edge-to-del-1
                                    :op-type :del-array-edge)
                    edge-to-del-2 (find-edge
                                   {:edges (:edges acc)
                                    :head-node-add-id splitting-node
                                    :tail-node-add-id (first next-path)})
                    del-op-2 (assoc base-op
                                    :add-id edge-to-del-2
                                    :op-type :del-array-edge)
                    edge-to-add {:add-id (make-add-id)
                                 :head-node-add-id (last path)
                                 :tail-node-add-id (first next-path)}
                    add-op (assoc base-op
                                  :add-id (:add-id edge-to-add)
                                  :op-type :add-array-edge
                                  :value edge-to-add)]
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
            (partition 2 1 paths))))

(defn get-conflict-resolution-info
  [{:keys [edges] :as arg}]
  (let [node->edge-info (make-node->edge-info edges)]
    (loop [node array-start-add-id
           acc {:edges edges
                :ops []}]
      (if (= array-end-add-id node)
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
              (recur combining-node new-acc))))))))

(defn get-non-linear-array-info [arg]
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
        {:keys [linear? node-add-ids]} ret]
    (when-not linear?
      (throw (ex-info "Array CRDT DAG is not linear after repair ops."
                      (u/sym-map arg cr-info edge-cleanup-info
                                 node-connect-info repair-ops))))
    (u/sym-map node-add-ids repair-ops linear-edges)))

(defn get-array-item-ids [arg]
  (let [{:keys [deleted-edges edges]} (get-array-edge-info arg)
        node->item-id (get-array-node-add-id->item-id arg)
        arg* (assoc arg
                    :edges edges
                    :deleted-edges deleted-edges
                    :live-nodes (set (keys node->item-id)))
        ret (get-linear-array-info arg*)
        {:keys [node-add-ids]} (if (:linear? ret)
                                 ret
                                 (get-non-linear-array-info arg*))]
    (mapv node->item-id node-add-ids)))

(defmethod get-crdt-val :array
  [{:keys [schema] :as arg}]
  (let [item-ids (get-array-item-ids arg)
        items-schema (l/schema-at-path schema [0])]
    (map (fn [item-id]
           (get-crdt-val (assoc arg
                                :item-id item-id
                                :schema items-schema)))
         item-ids)))

(defmethod get-crdt-val :array-kv
  [{:keys [k schema] :as arg}]
  (let [item-ids (get-array-item-ids arg)
        items-schema (l/schema-at-path schema [0])]
    (get-crdt-val (assoc arg
                         :item-id (nth item-ids k)
                         :schema items-schema))))

(defmethod get-crdt-val :single-value
  [{:keys [item-id data-store] :as arg}]
  (let [crdt (get-in data-store [:single-value-crdts item-id])]
    (get-single-value-crdt-val (assoc arg :crdt crdt))))
