(ns oncurrent.zeno.crdt.array
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.common :as c]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn delete-dangling-edges [arg]
  arg)

(defn connect-nodes-to-terminals [arg]
  arg)

(defn serialize-parallel-paths [arg]
  arg)

(defn repair-array [{:keys [crdt]}]
  (-> {:crdt crdt
       :ops #{}}
      (delete-dangling-edges)
      (connect-nodes-to-terminals)
      (serialize-parallel-paths)))

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
  [{:keys [edges live-nodes make-id] :as arg}]
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
                             edge {:add-id (make-id)
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
  [{:keys [edges deleted-edges live-nodes make-id]}]
  (let [*nodes-connected-to-start (atom #{c/array-start-node-id})
        *nodes-connected-to-end (atom #{c/array-end-node-id})
        new-edges (make-connecting-edges (u/sym-map deleted-edges
                                                    edges
                                                    live-nodes
                                                    make-id
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
                                    (= c/array-start-node-id head-node-id))
                                (or (live-nodes tail-node-id)
                                    (= c/array-end-node-id tail-node-id)))
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
           make-id
           ops
           paths
           schema
           splitting-node]}]
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
                  edge-to-add {:add-id (make-id)
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
      (loop [node c/array-start-node-id
             acc {:edges edges
                  :ops []}]
        (if (= c/array-end-node-id node)
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
      (when-not (node->edge-info c/array-start-node-id)
        (throw (ex-info (str "Array at path `" path "` does not have a start "
                             "node defined.")
                        (u/sym-map edges node->edge-info path))))
      (when-not (node->edge-info c/array-end-node-id)
        (throw (ex-info (str "Array at path `" path "` does not have an end "
                             "node defined.")
                        (u/sym-map edges node->edge-info path))))
      (loop [node-id c/array-start-node-id
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

            (= c/array-end-node-id child)
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

(defmethod c/get-value :array
  [{:keys [crdt make-id path schema]
    :or {make-id u/compact-random-uuid}
    :as arg}]
  (let [arg* (assoc arg :get-child-schema (fn [k]
                                            (l/schema-at-path schema [0])))
        v (c/associative-get-value arg*)]
    (if (seq path)
      v
      (let [child-schema (l/schema-at-path schema [0])
            {:keys [nodes-crdt]} crdt
            {:keys [deleted-edges edges]} (get-array-edge-info crdt)
            live-nodes (reduce-kv
                        (fn [acc node-id crdt*]
                          (let [v* (c/get-value
                                    (assoc arg*
                                           :crdt crdt*
                                           :schema child-schema))]
                            (if (nil? v*)
                              acc
                              (conj acc node-id))))
                        #{}
                        (:children crdt))
            info-arg (u/sym-map crdt
                                deleted-edges
                                edges
                                live-nodes
                                make-id
                                path)
            ret (get-linear-array-info info-arg)
            {:keys [ordered-node-ids]} (if (:linear? ret)
                                         ret
                                         (get-non-linear-array-info info-arg))]
        (mapv v ordered-node-ids)))))
