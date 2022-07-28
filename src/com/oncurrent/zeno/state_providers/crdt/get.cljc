(ns com.oncurrent.zeno.state-providers.crdt.get
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti get-value-info (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defn reverse-comparator [x y]
  (* -1 (compare x y)))

(defmethod get-value-info :single-value
  [{:keys [crdt growing-path shrinking-path]}]
  (if (seq shrinking-path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map growing-path shrinking-path)))
    (let [{:keys [add-id-to-value-info]} crdt
          value-info (case (count add-id-to-value-info)
                       0 nil
                       1 (-> add-id-to-value-info first second)
                       (->> (vals add-id-to-value-info)
                            (sort-by :sys-time-ms reverse-comparator)
                            (first)))]
      {:value (:value value-info)
       :exists? (boolean value-info)
       :norm-path growing-path})))

(defn get-child-value-info
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [[k & ks] shrinking-path
        child-crdt (get-in crdt [:children k])
        child-schema (if (keyword? k)
                       (l/child-schema schema k) ; record
                       (l/child-schema schema))] ; map
    (if child-crdt
      (get-value-info (assoc arg
                             :crdt child-crdt
                             :growing-path (conj growing-path k)
                             :schema child-schema
                             :shrinking-path ks))
      {:value nil
       :exists? false
       :norm-path growing-path})))

(defn associative-get-value-info
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [{:keys [children container-add-ids]} crdt
        is-record? (= :record (l/schema-type schema))
        child-ks (if is-record?
                   (map :name (:fields (l/edn schema)))
                   (keys children))]
    (if (empty? container-add-ids)
      {:value nil
       :exists? false
       :norm-path growing-path}
      (if (seq shrinking-path)
        (get-child-value-info arg)
        (let [v (reduce
                 (fn [acc k]
                   (let [vi (get-value-info
                             (assoc arg
                                    :crdt (get children k)
                                    :growing-path (conj growing-path k)
                                    :schema (if is-record?
                                              (l/child-schema schema k)
                                              (l/child-schema schema))))]
                     (if (and k (:exists? vi))
                       (assoc acc k (:value vi))
                       acc)))
                 {}
                 child-ks)]
          {:value v
           :exists? true
           :norm-path growing-path})))))

(defmethod get-value-info :array
  [{:keys [crdt growing-path path schema shrinking-path] :as arg}]
  (let [child-schema (l/child-schema schema)
        {:keys [children container-add-ids ordered-node-ids]} crdt]
    (if (empty? container-add-ids)
      {:value nil
       :exists? false
       :norm-path growing-path}
      (if (seq shrinking-path)
        (let [[i & sub-path] shrinking-path
              _ (when-not (int? i)
                  (throw (ex-info (str "Array index must be an integer. Got: `"
                                       (or i "nil") "`.")
                                  (u/sym-map path growing-path
                                             i shrinking-path sub-path))))
              array-len (count ordered-node-ids)
              norm-i (u/get-normalized-array-index (u/sym-map array-len i))
              k (get ordered-node-ids norm-i)]
          (get-value-info
           (assoc arg
                  :crdt (get-in crdt [:children k])
                  :growing-path (conj growing-path norm-i)
                  :schema child-schema
                  :shrinking-path sub-path)))
        (let [v (reduce
                 (fn [acc node-id]
                   (let [vi (get-value-info
                             (assoc arg
                                    :crdt (get children node-id)
                                    :growing-path (conj growing-path node-id)
                                    :schema child-schema))]
                     (if (:exists? vi)
                       (conj acc (:value vi))
                       acc)))
                 []
                 ordered-node-ids)]
          {:value v
           :exists? true
           :norm-path growing-path})))))

(defmethod get-value-info :map
  [arg]
  (associative-get-value-info arg))

(defmethod get-value-info :record
  [arg]
  (associative-get-value-info arg))

(defmethod get-value-info :union
  [{:keys [crdt growing-path schema] :as arg}]
  (let [member-schemas (l/member-schemas schema)
        ts-i-pairs (map (fn [union-branch]
                          (let [ts (->> (str "branch-"  union-branch
                                             "-sys-time-ms")
                                        (keyword)
                                        (get crdt))]
                            [(or ts 0) union-branch]))
                        (range (count member-schemas)))
        [ts i] (-> (sort-by first reverse-comparator ts-i-pairs)
                   (first))]
    (if (zero? ts)
      {:value nil
       :exists? false
       :norm-path growing-path}
      (let [branch-k (keyword (str "branch-" i))]
        (get-value-info (assoc arg
                               :crdt (get crdt branch-k)
                               :schema (nth member-schemas i)))))))

(defn get-in-state [{:keys [crdt data-schema path root] :as arg}]
  (when-not (keyword? root)
    (throw (ex-info (str "Bad `:root` arg in call to `get-in-state`. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to `get-in-state`. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map data-schema))))
  (when-not (= root (first path))
    (throw (ex-info (str "Mismatched root in `:path` arg. Should be `" root
                         "`. Got: `" (or (first path) "nil") "`."))))
  (-> (get-value-info (assoc arg
                             :growing-path [root]
                             :schema data-schema
                             :shrinking-path (rest path)))
      :value))