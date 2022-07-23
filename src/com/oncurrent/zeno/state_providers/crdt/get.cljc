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
  (let [[[add-id value-info] & rest] (:add-id-to-value-info crdt)]
    (when (seq rest)
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

(defmethod get-in-state* :map
  [{:keys [crdt growing-path schema shrinking-path] :as arg}]
  (let [{:keys [children container-add-ids]} crdt]
    (when (seq container-add-ids)
      (if (seq shrinking-path)
        (get-in-child-state arg)
        (reduce-kv
         (fn [acc k child-crdt]
           (assoc acc k
                  (get-in-state* (assoc arg
                                        :crdt child-crdt
                                        :growing-path (conj growing-path k)
                                        :schema (l/child-schema schema)))))
         {}
         children)))))

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
