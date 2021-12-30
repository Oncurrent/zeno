(ns oncurrent.zeno.crdts
  (:require
   [clojure.core.async :as ca]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as lu]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.storage :as storage]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def container-types #{:map :array :record})
(def array-end-add-id "-END-")
(def array-start-add-id "-START-")

(defn container-schema? [schema]
  (container-types (l/schema-type schema)))

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
        :else (throw (ex-info "Could not determine target item type."
                              (u/sym-map item-id schema-type)))))))

(defn <add-to-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/set-crdt-schema
                  (fn [crdt]
                    (if (-> (:deleted-add-ids crdt)
                            (get add-id))
                      crdt
                      (update crdt :current-value-infos conj arg)))))

(defn <del-from-crdt! [{:keys [add-id crdt-key storage]}]
  (storage/<swap! storage crdt-key schemas/set-crdt-schema
                  (fn [crdt]
                    (let [new-cvis (reduce
                                    (fn [acc cvi]
                                      (if (= add-id (:add-id cvi))
                                        acc
                                        (conj acc cvi)))
                                    []
                                    (:current-value-infos crdt))]
                      (-> crdt
                          (assoc :current-value-infos new-cvis)
                          (update :deleted-add-ids (fn [ids]
                                                     (if (seq ids)
                                                       (conj ids add-id)
                                                       #{add-id}))))))))

(defn <add-to-vhs-crdt!
  [{:keys [add-id crdt-key storage] :as arg}]
  (storage/<swap! storage crdt-key schemas/value-history-set-crdt-schema
                  (fn [crdt]
                    (let [{:keys [deleted-add-ids]} crdt
                          deleted? (get deleted-add-ids add-id)]
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

(defn get-most-recent [candidates]
  (reduce (fn [acc candidate]
            (if (> (:sys-time-ms candidate) (:sys-time-ms acc))
              candidate
              acc))
          (first candidates)
          candidates))

(defn <get-single-value-crdt-val
  [{:keys [crdt-key schema storage container-target-schema] :as arg}]
  (au/go
    (let [crdt (au/<? (storage/<get storage crdt-key schemas/set-crdt-schema))
          {:keys [current-value-infos]} crdt
          num-values (count current-value-infos)]
      (when (pos? num-values)
        (let [v (->> (get-most-recent current-value-infos)
                     (:serialized-value)
                     (storage/<serialized-value->value storage schema)
                     (au/<?))]
          (if-not container-target-schema
            v
            (au/<? (<get-crdt-val (-> arg
                                      (assoc :schema container-target-schema)
                                      (assoc :item-id v)
                                      (dissoc :container-target-schema)
                                      (dissoc :k))))))))))

(defn <get-kv [{:keys [k crdt-key schema item-id] :as arg}]
  (let [values-schema (l/schema-at-path schema [k])
        container-target? (container-schema? values-schema)]
    (<get-single-value-crdt-val (if container-target?
                                  (assoc arg
                                         :container-target-schema values-schema
                                         :schema l/string-schema)
                                  (assoc arg
                                         :schema values-schema)))))

(defmethod <get-crdt-val :map-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-map-key-value-crdt-key item-id k)]
    (<get-kv (assoc arg :crdt-key crdt-key))))

(defmethod <get-crdt-val :record-kv
  [{:keys [item-id k schema] :as arg}]
  (let [crdt-key (make-record-key-value-crdt-key item-id k)]
    (<get-kv (assoc arg :crdt-key crdt-key))))

(defmethod <get-crdt-val :map
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [keyset-k (make-map-keyset-crdt-key item-id)
          infos (some-> (storage/<get storage keyset-k schemas/set-crdt-schema)
                        (au/<?)
                        (:current-value-infos))
          num-infos (count infos)
          values-schema (l/schema-at-path schema ["x"])
          <get-kv (fn [{:keys [serialized-value]}]
                    (au/go
                      (let [k (au/<? (storage/<serialized-value->value
                                      storage
                                      l/string-schema
                                      serialized-value))
                            v (-> (assoc arg :k k)
                                  (<get-crdt-val)
                                  (au/<?))]
                        (u/sym-map k v))))
          ch (ca/merge (map <get-kv infos))]
      (if (zero? num-infos)
        {}
        (loop [out {}]
          (let [{:keys [k v] :as ret} (au/<? ch)
                new-out (assoc out k v)]
            (if (= num-infos (count new-out))
              new-out
              (recur new-out))))))))

(defmethod <get-crdt-val :record
  [{:keys [item-id schema storage] :as arg}]
  (au/go
    (let [field-ks (->> (l/edn schema)
                        (:fields)
                        (map :name))
          <get-kv (fn [k]
                    (au/go
                      (let [v (-> (assoc arg :k k)
                                  (<get-crdt-val)
                                  (au/<?))]
                        (u/sym-map k v))))
          ch (ca/merge (map <get-kv field-ks))
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

(defn make-node-add-id->parents [edges]
  (reduce (fn [acc {:keys [head-node-add-id tail-node-add-id]}]
            (update acc tail-node-add-id (fn [parents]
                                           (conj (or parents #{})
                                                 head-node-add-id))))
          {}
          edges))

(defn make-node->edge-info [edges]
  (reduce
   (fn [acc {:keys [head-node-add-id tail-node-add-id]}]
     (-> acc
         (update-in [head-node-add-id :children] (fn [children]
                                                   (conj (or children #{})
                                                         tail-node-add-id)))
         (update-in [tail-node-add-id :parents] (fn [parents]
                                                  (conj (or parents #{})
                                                        head-node-add-id)))))
   {:children []
    :parents []}
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
                           (loop [out []]
                             (let [new-out (conj out (au/<? ch))]
                               (if (= num-infos (count new-out))
                                 new-out
                                 (recur new-out)))))))]
      (cond-> {:edges []
               :deleted-edges []}
        (pos? (count cur-infos))
        (assoc :edges (au/<? (<get-edges cur-infos)))

        (pos? (count del-infos))
        (assoc :deleted-edges (au/<? (<get-edges del-infos)))))))

(defn <get-array-node-add-id->value
  [{:keys [item-id storage schema]}]
  (au/go
    (let [k (make-array-nodes-crdt-key item-id)
          infos (some-> (storage/<get storage k schemas/set-crdt-schema)
                        (au/<?)
                        (:current-value-infos))
          num-infos (count infos)
          item-schema (l/schema-at-path schema [0])
          <get-node (fn [{:keys [serialized-value add-id]}]
                      (au/go
                        (let [value (au/<? (storage/<serialized-value->value
                                            storage
                                            item-schema
                                            serialized-value))]
                          (u/sym-map add-id value))))
          ch (ca/merge (map <get-node infos))]
      (if (zero? num-infos)
        {}
        (loop [out {}]
          (let [{:keys [add-id value]} (au/<? ch)
                new-out (assoc out add-id value)]
            (if (= num-infos (count new-out))
              new-out
              (recur new-out))))))))

(defn <get-linear-array-info
  [{:keys [edges node->v] :as arg}]
  (au/go
    (let [node->edge-info (make-node->edge-info edges)]
      (loop [node-id array-start-add-id
             nodes []]
        (let [children (get-in node->edge-info [node-id :children])
              child (first children)]
          (cond
            (> (count children) 1)
            {:linear false}

            (= array-end-add-id child)
            (let [linear? true
                  v (mapv node->v nodes)]
              (u/sym-map linear? v))

            :else
            (recur child (conj nodes child))))))))

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
                   {:disconnected-edges []
                    :connected-edges []}
                   edges)
        op-type :del-array-edge
        base-op (u/sym-map item-id op-type)
        {:keys [connected-edges disconnected-edges]} edge-info]
    {:edge-cleanup-ops (map #(assoc base-op :add-id (:add-id %))
                            disconnected-edges)
     :edges-after-cleanup connected-edges
     :deleted-edges-after-cleanup (vec (concat deleted-edges
                                               disconnected-edges))}))

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
               :new-edges []}
              [:start :end])
      :new-edges))

(defn <get-node-connect-info
  [{:keys [edges deleted-edges item-id live-nodes make-add-id storage]}]
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
          <ser-edge #(storage/<value->serialized-value
                      storage schemas/array-edge-schema %)
          ops (loop [i 0
                     out []]
                (if (zero? num-edges)
                  []
                  (let [edge (nth new-edges i)
                        op (assoc base-op
                                  :add-id (:add-id edge)
                                  :serialized-value (au/<? (<ser-edge edge)))
                        new-out (conj out op)]
                    (if (= num-edges (count new-out))
                      new-out
                      (recur (inc i) new-out)))))]
      {:node-connect-ops ops
       :edges-after-node-connect (concat edges new-edges)})))

(defn get-conflict-resolution-info
  [arg]
  )

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
          conflict-resolution-info (get-conflict-resolution-info
                                    (assoc arg :edges edges-after-node-connect))
          {:keys [conflict-resolution-ops
                  linear-edges]} conflict-resolution-info
          v :adlfkasj
          repair-ops (concat edge-cleanup-ops
                             node-connect-ops
                             conflict-resolution-ops)]
      (log/info (str "########################:\n"
                     (u/pprint-str
                      (u/sym-map edge-cleanup-info
                                 node-connect-info
                                 conflict-resolution-info
                                 v
                                 repair-ops
                                 linear-edges))))
      (u/sym-map v repair-ops linear-edges))))

(defmethod <get-crdt-val :array
  [{:keys [make-add-id]
    :or {make-add-id u/compact-random-uuid}
    :as arg}]
  (au/go
    (let [{:keys [deleted-edges edges]} (au/<? (<get-array-edge-info arg))
          node->v (au/<? (<get-array-node-add-id->value arg))
          arg* (assoc arg
                      :edges edges
                      :deleted-edges deleted-edges
                      :make-add-id make-add-id
                      :live-nodes (set (keys node->v))
                      :node->v node->v)
          {:keys [linear? v] :as ret} (au/<? (<get-linear-array-info arg*))]
      (if linear?
        v
        (let [{:keys [v]} (au/<? (<get-non-linear-array-info arg*))]
          v)))))


#_(defn path-to-combining-node [node->children node->parents first-node]
    (loop [cur-node first-node
           out []]
      (if (> (count (node->parents cur-node)) 1) ; This is a combining node
        {:path out
         :combining-node cur-node}
        (let [new-node (first (node->children cur-node))
              new-out (conj out cur-node)]
          (recur new-node new-out)))))

#_(defn walk* [node->children node->parents first-node]
    (loop [cur-node first-node
           out []]
      (when (nil? cur-node)
        (throw (ex-info "!!!!" (u/sym-map cur-node out))))
      (if (= array-end-add-id cur-node)
        out
        (let [new-out (if (= array-start-add-id cur-node)
                        out
                        (conj out cur-node))
              ;; We sort by node add-id to guarantee consistent child order
              children (sort (node->children cur-node))]
          (if (> (count children) 1) ; This is a splitting node
            (let [path-infos (map #(path-to-combining-node
                                    node->children node->parents %)
                                  children)
                  {:keys [combining-node]} (first path-infos)
                  paths (mapcat :path path-infos)
                  path-to-end (walk* node->children node->parents combining-node)]
              (concat new-out paths path-to-end))
            (recur (first children) new-out))))))

#_
(defn walk [edges]
  (let [node->children (make-node-add-id->children edges)
        node->parents (make-node-add-id->parents edges)]
    (walk* node->children node->parents array-start-add-id)))
