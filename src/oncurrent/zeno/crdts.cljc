(ns oncurrent.zeno.crdts
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:map :array :record})
(def array-end-add-id "-END-")
(def array-start-add-id "-START-")

(defn make-map-keyset-crdt-key [item-id]
  (str storage/map-keyset-crdt-key-prefix item-id))

(defn make-map-key-value-crdt-key [item-id k]
  (str storage/map-key-value-crdt-key-prefix item-id "-" k))

(defn make-record-key-value-crdt-key [item-id k]
  (when-not (keyword? k)
    (throw (ex-info (str "Record key must be a keyword. Got: `" k "`.")
                    (u/sym-map k item-id))))
  (str storage/record-key-value-crdt-key-prefix item-id "-"
       (namespace k) "-" (name k)))

(defn make-array-edges-crdt-key [item-id]
  (str storage/array-edges-crdt-key-prefix item-id))

(defn make-array-nodes-crdt-key [item-id]
  (str storage/array-nodes-crdt-key-prefix item-id))

(defn make-array-node-item-crdt-key [item-id]
  (str storage/array-node-item-crdt-key-prefix item-id))

(defn make-single-value-crdt-key [item-id]
  (str storage/single-value-crdt-key-prefix item-id))

(defmulti <apply-op! #(-> % :op :op-type))

(defmulti <get-crdt-val
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

(def immutable-crdt-value-keys [:add-id :sys-time-ms :union-branch])

(defn same-cv-info? [x y]
  (and x
       y
       (= (select-keys x immutable-crdt-value-keys)
          (select-keys y immutable-crdt-value-keys))
       (ba/equivalent-byte-arrays? (:serialized-value x)
                                   (:serialized-value y))))

(defn <add-to-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/set-crdt-schema
                  (fn [{:keys [current-add-id-to-value-info
                               deleted-add-ids]
                        :as crdt}]
                    (let [info (get current-add-id-to-value-info add-id)
                          same? (same-cv-info? info arg)
                          deleted? (get deleted-add-ids add-id)]
                      (when (and info (not same?))
                        (throw (ex-info
                                (str "Attempt to reuse an existing add-id "
                                     "(`" add-id "`) to add different info "
                                     "to CRDT `" crdt-key "`.")
                                (u/sym-map add-id crdt-key info))))
                      (if (or deleted? same?)
                        crdt
                        (update crdt :current-add-id-to-value-info
                                assoc add-id arg))))))

(defn <del-from-crdt! [{:keys [add-id crdt-key storage]}]
  (storage/<swap!
   storage crdt-key
   schemas/set-crdt-schema
   (fn [crdt]
     (-> crdt
         (update :current-add-id-to-value-info dissoc add-id)
         (update :deleted-add-ids (fn [ids]
                                    (if (seq ids)
                                      (conj ids add-id)
                                      #{add-id})))))))

(defn <add-to-vhs-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/value-history-set-crdt-schema
                  (fn [{:keys [add-id-to-value-info
                               current-add-ids
                               deleted-add-ids] :as crdt}]
                    (let [deleted? (get deleted-add-ids add-id)
                          current? (get current-add-ids add-id)
                          info (get add-id-to-value-info add-id)
                          same? (same-cv-info? info arg)]
                      (when (and info (not same?))
                        (throw (ex-info
                                (str "Attempt to reuse an existing add-id "
                                     "(`" add-id "`) to add different info "
                                     "to CRDT `" crdt-key "`.")
                                (u/sym-map add-id crdt-key info))))
                      (cond-> (assoc-in crdt [:add-id-to-value-info add-id] arg)
                        (not deleted?)
                        (update :current-add-ids (fn [ids]
                                                   (if (seq ids)
                                                     (conj ids add-id)
                                                     #{add-id}))))))))

(defn <del-from-vhs-crdt! [{:keys [add-id crdt-key storage]}]
  (storage/<swap! storage crdt-key schemas/value-history-set-crdt-schema
                  (fn [crdt]
                    (-> crdt
                        (update :current-add-ids disj add-id)
                        (update :deleted-add-ids (fn [ids]
                                                   (if (seq ids)
                                                     (conj ids add-id)
                                                     #{add-id})))))))

(defmethod <apply-op! :add-single-value
  [{:keys [storage op]}]
  (let [{:keys [item-id]} op
        arg (assoc op
                   :crdt-key (make-single-value-crdt-key item-id)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-single-value
  [{:keys [storage op]}]
  (let [{:keys [item-id]} op
        arg (assoc op
                   :crdt-key (make-single-value-crdt-key item-id)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-record-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key item-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-record-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-record-key-value-crdt-key item-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key item-id k)
                   :storage storage)]
    (<add-to-crdt! arg)))

(defmethod <apply-op! :del-map-key-value
  [{:keys [storage op]}]
  (let [{:keys [item-id k]} op
        arg (assoc op
                   :crdt-key (make-map-key-value-crdt-key item-id k)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-map-key
  [{:keys [storage op]}]
  (au/go
    (let [{:keys [item-id k]} op
          ser-k (au/<? (storage/<value->serialized-value
                        storage l/string-schema k))
          arg (assoc op
                     :crdt-key (make-map-keyset-crdt-key item-id)
                     :serialized-value ser-k
                     :storage storage)]
      (au/<? (<add-to-crdt! arg)))))

(defmethod <apply-op! :del-map-key
  [{:keys [storage op]}]
  (let [arg (assoc op
                   :crdt-key (make-map-keyset-crdt-key (:item-id op))
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :add-array-node
  [{:keys [storage op]}]
  (au/go
    (let [{:keys [item-id]} op
          arg (assoc op
                     :crdt-key (make-array-nodes-crdt-key item-id)
                     :storage storage)]
      (au/<? (<add-to-crdt! arg)))))

(defmethod <apply-op! :add-array-edge
  [{:keys [storage op]}]
  (au/go
    (let [{:keys [item-id]} op
          arg (assoc op
                     :crdt-key (make-array-edges-crdt-key item-id)
                     :storage storage)]
      (au/<? (<add-to-vhs-crdt! arg)))))

(defmethod <apply-op! :del-array-node
  [{:keys [storage op]}]
  (let [{:keys [item-id]} op
        arg (assoc op
                   :crdt-key (make-array-nodes-crdt-key item-id)
                   :storage storage)]
    (<del-from-crdt! arg)))

(defmethod <apply-op! :del-array-edge
  [{:keys [storage op]}]
  (let [{:keys [item-id]} op
        arg (assoc op
                   :crdt-key (make-array-edges-crdt-key item-id)
                   :storage storage)]
    (<del-from-vhs-crdt! arg)))

(defn <apply-ops!
  [{:keys [ops storage]}]
  (au/go
    (let [num-ops (count ops)
          <apply-op!* (fn [op]
                        (<apply-op! (u/sym-map op storage)))
          ch (ca/merge (map <apply-op!* ops))]
      (if (zero? num-ops)
        true
        (loop [num-done 0]
          (au/<? ch)
          (let [new-num-done (inc num-done)]
            (if (= num-ops new-num-done)
              true
              (recur new-num-done))))))))

(defn get-most-recent [current-add-id-to-value-info]
  (->> (vals current-add-id-to-value-info)
       (sort-by :sys-time-ms)
       (last)))

(defn <get-single-value-crdt-val
  [{:keys [crdt-key item-id schema storage] :as arg}]
  (au/go
    (let [crdt (au/<? (storage/<get storage crdt-key schemas/set-crdt-schema))
          {:keys [current-add-id-to-value-info]} crdt
          num-values (count current-add-id-to-value-info)]
      (when (pos? num-values)
        (let [schema-type (l/schema-type schema)
              value-info (get-most-recent current-add-id-to-value-info)
              {:keys [serialized-value union-branch]} value-info]
          (cond
            (and (= :union schema-type) union-branch)
            (let [member-schema (-> (l/edn schema)
                                    (nth union-branch)
                                    (l/edn->schema))
                  member-schema-type (l/schema-type member-schema)]
              (if-not (container-types member-schema-type)
                (au/<? (storage/<serialized-value->value storage schema))
                (let [item-id* (au/<? (storage/<serialized-value->value
                                       storage
                                       l/string-schema
                                       serialized-value))]
                  (au/<? (<get-crdt-val (-> arg
                                            (assoc :schema member-schema)
                                            (assoc :item-id item-id*)))))))

            (container-types schema-type)
            (let [item-id* (au/<? (storage/<serialized-value->value
                                   storage l/string-schema serialized-value))]
              (au/<? (<get-crdt-val (assoc arg :item-id item-id*))))

            :else
            (au/<? (storage/<serialized-value->value
                    storage schema serialized-value))))))))

(defmethod <get-crdt-val :map-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-map-key-value-crdt-key item-id k)
        values-schema (l/schema-at-path schema [k])]
    (<get-single-value-crdt-val (-> arg
                                    (assoc :crdt-key crdt-key)
                                    (assoc :schema values-schema)
                                    (dissoc :k)))))

(defmethod <get-crdt-val :record-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-record-key-value-crdt-key item-id k)
        field-schema (l/schema-at-path schema [k])]
    (<get-single-value-crdt-val (-> arg
                                    (assoc :crdt-key crdt-key)
                                    (assoc :schema field-schema)
                                    (dissoc :k)))))

(defmethod <get-crdt-val :map
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [keyset-k (make-map-keyset-crdt-key item-id)
          infos (some-> (storage/<get storage
                                      keyset-k
                                      schemas/set-crdt-schema)
                        (au/<?)
                        (:current-add-id-to-value-info)
                        (vals))
          values-schema (l/schema-at-path schema ["x"])
          <get-kv-info (fn [{:keys [serialized-value]}]
                         (au/go
                           (let [k (au/<? (storage/<serialized-value->value
                                           storage
                                           l/string-schema
                                           serialized-value))
                                 v (-> (assoc arg :k k)
                                       (<get-crdt-val)
                                       (au/<?))]
                             (u/sym-map k v))))
          ch (ca/merge (map <get-kv-info infos))
          last-i (dec (count infos))]
      (if (neg? last-i)
        {}
        (loop [i 0
               out {}]
          (let [{:keys [k v] :as ret} (au/<? ch)
                new-out (assoc out k v)]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defmethod <get-crdt-val :record
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [field-ks (->> (l/edn schema)
                        (:fields)
                        (map :name))
          <get-kv-info (fn [k]
                         (au/go
                           (let [v (-> (assoc arg :k k)
                                       (<get-crdt-val)
                                       (au/<?))]
                             (u/sym-map k v))))
          ch (ca/merge (map <get-kv-info field-ks))
          num-fields (count field-ks)]
      (loop [fields-processed 0
             out {}]
        (let [{:keys [k v] :as ret} (au/<? ch)
              new-out (if (nil? v)
                        out
                        (assoc out k v))
              new-fields-processed (inc fields-processed)]
          (if (= num-fields new-fields-processed)
            new-out
            (recur new-fields-processed new-out)))))))

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

(defn <get-array-edge-info
  [{:keys [item-id storage]}]
  (au/go
    (let [k (make-array-edges-crdt-key item-id)
          crdt (au/<? (storage/<get storage k
                                    schemas/value-history-set-crdt-schema))
          {:keys [add-id-to-value-info current-add-ids deleted-add-ids]} crdt
          cur-infos (mapv (fn [add-id]
                            (-> (get add-id-to-value-info add-id)
                                (assoc :add-id add-id)))
                          current-add-ids)
          del-infos (mapv (fn [add-id]
                            (-> (get add-id-to-value-info add-id)
                                (assoc :add-id add-id)))
                          deleted-add-ids)
          <get-edge (fn [{:keys [serialized-value add-id]}]
                      (au/go
                        (let [edge (au/<? (storage/<serialized-value->value
                                           storage
                                           schemas/array-edge-schema
                                           serialized-value))]
                          (assoc edge :add-id add-id))))
          <get-edges (fn [infos]
                       (au/go
                         (let [num-infos (count infos)
                               ch (ca/merge (map <get-edge infos))]
                           (loop [out #{}]
                             (let [new-out (conj out (au/<? ch))]
                               (if (= num-infos (count new-out))
                                 new-out
                                 (recur new-out)))))))]
      (cond-> {:edges #{}
               :deleted-edges #{}}
        (pos? (count cur-infos))
        (assoc :edges (au/<? (<get-edges cur-infos)))

        (pos? (count del-infos))
        (assoc :deleted-edges (au/<? (<get-edges del-infos)))))))

(defn <get-array-node-add-id->item-id
  [{:keys [item-id storage schema] :as arg}]
  (au/go
    (let [k (make-array-nodes-crdt-key item-id)
          add-id->info (some-> (storage/<get storage k schemas/set-crdt-schema)
                               (au/<?)
                               (:current-add-id-to-value-info))
          item-schema (l/schema-at-path schema [0])
          <get-node (fn [[add-id {:keys [serialized-value]}]]
                      (au/go
                        (let [item-id (au/<? (storage/<serialized-value->value
                                              storage
                                              l/string-schema
                                              serialized-value))]
                          (u/sym-map add-id item-id))))
          ch (ca/merge (map <get-node add-id->info))
          last-i (dec (count add-id->info))]
      (if (neg? last-i)
        {}
        (loop [i 0
               out {}]
          (let [{:keys [add-id item-id]} (au/<? ch)
                new-out (assoc out add-id item-id)]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defn <get-linear-array-info
  [{:keys [edges item-id] :as arg}]
  (au/go
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
              (recur child (conj node-add-ids child)))))))))

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

(defn <get-node-connect-info
  [{:keys [edges deleted-edges item-id live-nodes make-add-id storage]
    :or {make-add-id u/compact-random-uuid}}]
  (au/go
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
          num-edges (count new-edges)
          new-edges-vec (vec new-edges)
          <ser-edge #(storage/<value->serialized-value
                      storage schemas/array-edge-schema %)
          ops (loop [i 0
                     out []]
                (if (zero? num-edges)
                  []
                  (let [edge (nth new-edges-vec i)
                        op (assoc base-op
                                  :add-id (:add-id edge)
                                  :serialized-value (au/<? (<ser-edge edge)))
                        new-out (conj out op)]
                    (if (= num-edges (count new-out))
                      new-out
                      (recur (inc i) new-out)))))]
      {:node-connect-ops ops
       :edges-after-node-connect (set/union edges new-edges)})))

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

(defn <get-conflict-resolution-op-infos
  [{:keys [combining-node
           edges
           item-id
           make-add-id
           ops
           paths
           schema
           splitting-node
           storage]
    :or {make-add-id u/compact-random-uuid}}]
  (au/go
    (let [num-paths (count paths)
          last-i (- num-paths 2) ; Skip the last path
          base-op (u/sym-map item-id schema)
          <ser-edge #(storage/<value->serialized-value
                      storage
                      schemas/array-edge-schema
                      %)]
      (loop [i 0
             acc (u/sym-map edges ops)]
        (let [path (nth paths i)
              next-path (nth paths (inc i))
              edge-to-del-1 (find-edge {:edges (:edges acc)
                                        :head-node-add-id (last path)
                                        :tail-node-add-id combining-node})
              del-op-1 (assoc base-op
                              :add-id edge-to-del-1
                              :op-type :del-array-edge)
              edge-to-del-2 (find-edge {:edges (:edges acc)
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
                            :serialized-value (au/<? (<ser-edge edge-to-add)))
              new-acc (-> acc
                          (update :edges #(-> %
                                              (disj edge-to-del-1)
                                              (disj edge-to-del-2)
                                              (conj edge-to-add)))
                          (update :ops #(-> %
                                            (conj del-op-1)
                                            (conj del-op-2)
                                            (conj add-op))))]
          (if (= last-i i)
            new-acc
            (recur (inc i) new-acc)))))))

(defn <get-conflict-resolution-info
  [{:keys [edges] :as arg}]
  (au/go
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
                    new-acc (au/<? (<get-conflict-resolution-op-infos
                                    (assoc arg
                                           :combining-node combining-node
                                           :edges (:edges acc)
                                           :ops (:ops acc)
                                           :paths (map :path path-infos)
                                           :splitting-node node)))]
                (recur combining-node new-acc)))))))))

(defn <get-non-linear-array-info
  [arg]
  (au/go
    (let [edge-cleanup-info (get-edge-cleanup-info arg)
          {:keys [deleted-edges-after-cleanup
                  edge-cleanup-ops
                  edges-after-cleanup]} edge-cleanup-info
          arg* (assoc arg
                      :edges edges-after-cleanup
                      :deleted-edges deleted-edges-after-cleanup)
          node-connect-info (au/<? (<get-node-connect-info arg*))
          {:keys [node-connect-ops
                  edges-after-node-connect]} node-connect-info
          cr-info (au/<? (<get-conflict-resolution-info
                          (assoc arg :edges edges-after-node-connect)))
          repair-ops (concat edge-cleanup-ops
                             node-connect-ops
                             (:ops cr-info))
          linear-edges (:edges cr-info)
          ret (au/<? (<get-linear-array-info (assoc arg :edges linear-edges)))
          {:keys [linear? node-add-ids]} ret]
      (when-not linear?
        (throw (ex-info "Array CRDT DAG is not linear after repair ops."
                        (u/sym-map arg cr-info edge-cleanup-info
                                   node-connect-info repair-ops))))
      (u/sym-map node-add-ids repair-ops linear-edges))))

(defn <get-array-item-ids [arg]
  (au/go
    (let [{:keys [deleted-edges edges]} (au/<? (<get-array-edge-info arg))
          node->item-id (au/<? (<get-array-node-add-id->item-id arg))
          arg* (assoc arg
                      :edges edges
                      :deleted-edges deleted-edges
                      :live-nodes (set (keys node->item-id)))
          ret (au/<? (<get-linear-array-info arg*))
          {:keys [node-add-ids]} (if (:linear? ret)
                                   ret
                                   (au/<? (<get-non-linear-array-info arg*)))]
      (mapv node->item-id node-add-ids))))

(defmethod <get-crdt-val :array
  [{:keys [schema] :as arg}]
  (au/go
    (let [item-ids (au/<? (<get-array-item-ids arg))
          last-i (dec (count item-ids))
          items-schema (l/schema-at-path schema [0])]
      (if (neg? last-i)
        []
        (loop [i 0
               out []]
          (let [arg* (assoc arg
                            :item-id (nth item-ids i)
                            :schema items-schema)
                v (au/<? (<get-crdt-val arg*))
                new-out (conj out v)]
            (if (= last-i i)
              new-out
              (recur (inc i) new-out))))))))

(defmethod <get-crdt-val :array-kv
  [{:keys [k schema] :as arg}]
  (au/go
    (let [item-ids (au/<? (<get-array-item-ids arg))
          items-schema (l/schema-at-path schema [0])
          arg* (assoc arg
                      :item-id (nth item-ids k)
                      :schema items-schema)]
      (au/<? (<get-crdt-val arg*)))))

(defmethod <get-crdt-val :single-value
  [{:keys [item-id schema] :as arg}]
  (let [crdt-key (make-single-value-crdt-key item-id)]
    (<get-single-value-crdt-val (assoc arg :crdt-key crdt-key))))
