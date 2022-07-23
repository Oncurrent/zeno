(ns com.oncurrent.zeno.state-providers.crdt.commands
  (:require
   [clojure.set :as set]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def insert-crdt-ops #{:zeno/insert-after
                       :zeno/insert-before
                       :zeno/insert-range-after
                       :zeno/insert-range-before})

(defmulti process-cmd* (fn [{:keys [cmd]}]
                         (let [{:zeno/keys [op]} cmd]
                           (if (insert-crdt-ops op)
                             :zeno/insert*
                             op))))

(defmulti get-delete-ops (fn [{:keys [schema]}]
                           (c/schema->dispatch-type schema)))

(defmulti get-add-ops (fn [{:keys [schema]}]
                        (c/schema->dispatch-type schema)))

(defmethod get-delete-ops :single-value
  [{:keys [crdt growing-path] :as arg}]
  (reduce (fn [acc add-id]
            (conj acc {:add-id add-id
                       :op-type :delete-value
                       :op-path growing-path}))
          #{}
          (keys (:add-id-to-value-info crdt))))

(defmethod get-add-ops :single-value
  [{:keys [cmd-arg make-id growing-path sys-time-ms schema] :as arg}]
  #{{:add-id (make-id)
     :op-type :add-value
     :op-path growing-path
     :serialized-value (l/serialize schema cmd-arg)
     :sys-time-ms sys-time-ms
     :value cmd-arg}})

(defn get-delete-container-ops [{:keys [crdt growing-path]}]
  (reduce (fn [acc add-id]
            (conj acc {:add-id add-id
                       :op-type :delete-container
                       :op-path growing-path}))
          #{}
          (:container-add-ids crdt)))

(defn get-add-container-ops
  [{:keys [crdt growing-path make-id sys-time-ms]}]
  (let [{:keys [container-add-ids]} crdt]
    (if (seq container-add-ids)
      #{}
      #{{:add-id (make-id)
         :op-path growing-path
         :op-type :add-container
         :sys-time-ms sys-time-ms}})))

(defmethod get-delete-ops :map
  [{:keys [cmd-path cmd-type crdt growing-path shrinking-path schema]
    :as arg}]
  (if (empty? shrinking-path)
    (reduce-kv (fn [acc k child-crdt]
                 (set/union acc (get-delete-ops
                                 (assoc arg
                                        :crdt child-crdt
                                        :growing-path (conj growing-path k)
                                        :schema (l/child-schema schema)))))
               (get-delete-container-ops arg)
               (:children crdt))
    (let [[k & ks] shrinking-path]
      (get-delete-ops (assoc arg
                             :crdt (get-in crdt [:children k])
                             :growing-path (conj growing-path k)
                             :schema (l/child-schema schema)
                             :shrinking-path ks)))))

(defmethod get-add-ops :map
  [{:keys [cmd cmd-arg cmd-path cmd-type crdt growing-path
           schema shrinking-path]
    :as arg}]
  (let [container-ops (get-add-container-ops arg)]
    (if (empty? shrinking-path)
      (do
        (when (and (= :zeno/set cmd-type)
                   (not (map? cmd-arg)))
          (throw (ex-info
                  (str "The given :zeno/arg (`" (or cmd-arg "nil") "`) "
                       "must be a map for the given command (`"
                       cmd "`).")
                  (u/sym-map cmd-path cmd-arg))))
        (reduce-kv (fn [acc k v]
                     (set/union acc (get-add-ops
                                     (assoc arg
                                            :cmd-arg v
                                            :crdt (get-in crdt
                                                          [:children k])
                                            :growing-path (conj growing-path k)
                                            :schema (l/child-schema schema)))))
                   container-ops
                   cmd-arg))
      (let [[k & ks] shrinking-path
            add-ops (get-add-ops (assoc arg
                                        :crdt (get-in crdt
                                                      [:children k])
                                        :growing-path (conj growing-path k)
                                        :schema (l/child-schema schema)
                                        :shrinking-path ks))]
        (set/union container-ops add-ops)))))

(defmethod process-cmd* :zeno/set
  [{:keys [data-schema crdt] :as arg}]
  (let [crdt-ops (set/union (get-delete-ops arg)
                            (get-add-ops arg))
        crdt (apply-ops/apply-ops (assoc arg :crdt-ops crdt-ops))]
    (u/sym-map crdt crdt-ops)))

(defn process-cmd [{:keys [cmd crdt data-schema root] :as arg}]
  (let [cmd-path (:zeno/path cmd)]
    (when-not (= root (first cmd-path))
      (throw (ex-info (str "Command path root `" (first cmd-path)
                           "` does not match given root `" root "`.")
                      (u/sym-map root cmd-path))))
    (process-cmd* (assoc arg
                         :cmd-arg (:zeno/arg cmd)
                         :cmd-path cmd-path
                         :cmd-type (:zeno/op cmd)
                         :growing-path [root]
                         :schema data-schema
                         :shrinking-path (rest cmd-path)))))

(defn process-cmds [{:keys [cmds crdt data-schema make-id root] :as arg}]
  (when-not (keyword? root)
    (throw (ex-info (str "Bad `:root` arg in call to `process-cmds`. Got: `"
                         (or root "nil") "`.")
                    (u/sym-map cmds root))))
  (when-not (l/schema? data-schema)
    (throw (ex-info (str "Bad `:data-schema` arg in call to `process-cmds`. "
                         "Got: `" (or data-schema "nil") "`.")
                    (u/sym-map cmds data-schema))))
  (let [make-id* (or make-id u/compact-random-uuid)
        sys-time-ms (or (:sys-time-ms arg) (u/current-time-ms))]
    (reduce (fn [acc cmd]
              (let [ret (process-cmd (assoc arg
                                            :crdt (:crdt acc)
                                            :cmd cmd
                                            :make-id make-id*
                                            :sys-time-ms sys-time-ms))]
                (-> acc
                    (assoc :crdt (:crdt ret))
                    (update :crdt-ops set/union (:crdt-ops ret)))))
            {:crdt crdt
             :crdt-ops #{}}
            cmds)))
