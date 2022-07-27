(ns com.oncurrent.zeno.state-providers.crdt.get
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti get-value-info (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmethod get-value-info :single-value
  [{:keys [crdt growing-path shrinking-path]}]
  (if (seq shrinking-path)
    (throw (ex-info "Can't index into a single-value CRDT."
                    (u/sym-map growing-path shrinking-path)))
    (let [[[add-id value-info] & others] (:add-id-to-value-info crdt)]
      (when (seq others)
        (throw (ex-info "CRDT needs repair"
                        {:crdt crdt
                         :path growing-path})))
      {:value (:value value-info)
       :exists? (boolean value-info)
       :norm-path growing-path})))

(defn get-child-value-info
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [[k & ks] shrinking-path
        child-crdt (get-in crdt [:children k])
        child-schema (if (keyword? k)
                       (l/child-schema schema k) ; record
                       (l/child-schema schema))] ; map / array
    (when child-crdt
      (get-value-info (assoc arg
                             :crdt child-crdt
                             :growing-path (conj growing-path k)
                             :schema child-schema
                             :shrinking-path ks)))))

(defn associative-get-value-info
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [{:keys [children container-add-ids]} crdt
        record? (= :record (l/schema-type schema))
        child-ks (if record?
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
                                    :schema (if record?
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
        [ts i] (-> (sort-by first ts-i-pairs)
                   (reverse)
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
