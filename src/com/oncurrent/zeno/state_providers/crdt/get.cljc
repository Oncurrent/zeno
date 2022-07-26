(ns com.oncurrent.zeno.state-providers.crdt.get
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [taoensso.timbre :as log]))

(defmulti get-in-state* (fn [{:keys [schema]}]
                          (c/schema->dispatch-type schema)))

(defmethod get-in-state* :single-value
  [{:keys [crdt growing-path]}]
  (let [[[add-id value-info] & others] (:add-id-to-value-info crdt)]
    (when (seq others)
      (throw (ex-info "CRDT needs repair"
                      {:crdt crdt
                       :path growing-path})))
    (:value value-info)))

(defn get-in-child-state
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [[k & ks] shrinking-path
        child-crdt (get-in crdt [:children k])
        child-schema (if (keyword? k)
                       (l/child-schema schema k) ; record
                       (l/child-schema schema))] ; map
    (when child-crdt
      (get-in-state* (assoc arg
                            :crdt child-crdt
                            :growing-path (conj growing-path k)
                            :schema child-schema
                            :shrinking-path ks)))))

(defn associative-get-in-state*
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [{:keys [children container-add-ids]} crdt
        record? (= :record (l/schema-type schema))
        child-ks (if record?
                   (map :name (:fields (l/edn schema)))
                   (keys children))]
    (when (seq container-add-ids)
      (if (seq shrinking-path)
        (get-in-child-state arg)
        (reduce
         (fn [acc k]
           (assoc acc k
                  (get-in-state* (assoc arg
                                        :crdt (get children k)
                                        :growing-path (conj growing-path k)
                                        :schema (if record?
                                                  (l/child-schema schema k)
                                                  (l/child-schema schema))))))
         {}
         child-ks)))))

(defmethod get-in-state* :map
  [arg]
  (associative-get-in-state* arg))

(defmethod get-in-state* :record
  [arg]
  (associative-get-in-state* arg))

(defmethod get-in-state* :union
  [{:keys [crdt schema] :as arg}]
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
    (when (pos? ts)
      (let [branch-k (keyword (str "branch-" i))]
        (get-in-state* (assoc arg
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
  (get-in-state* (assoc arg
                        :growing-path [root]
                        :schema data-schema
                        :shrinking-path (rest path))))
