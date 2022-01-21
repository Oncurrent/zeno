(ns oncurrent.zeno.groups
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.common :as crdt-c]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def everyone-kw :zeno/everyone)
(def everyone-str (str everyone-kw))

(l/def-enum-schema role-schema
  :admin :member)

(l/def-enum-schema member-type-schema
  :actor :group)

(l/def-record-schema member-info-schema
  [:role role-schema]
  [:type member-type-schema])

(def group-member-schema
  (l/map-schema ; subject-id
   member-info-schema))

(def groups-schema
  (l/map-schema ; group-id
   group-member-schema))

(def valid-roles
  (-> role-schema l/edn :symbols set))

(def valid-types
  (-> member-type-schema l/edn :symbols set))

(defmulti apply-op :op-type)

(defn check-arg [{:keys [path value] :as arg}]
  (let [[groups-k group-id subject-id k union-branch] path]
    (when-not (= :groups (first path))
      (throw (ex-info (str "Group path must begin with `:groups`. Got `"
                           groups-k ".")
                      arg)))
    (when (= 5 (count path))
      (when (and (= :role k)
                 value
                 (not (valid-roles value)))
        (throw (ex-info (str "Unknown group member role `" value
                             "`. Must be one of `" valid-roles "`.")
                        arg)))
      (when (and (= :type k)
                 value
                 (not (valid-types value)))
        (throw (ex-info (str "Unknown group member type `" value
                             "`. Must be one of `" valid-types "`.")
                        arg))))))

(defmethod apply-op :add-value
  [{:keys [group-store path] :as arg}]
  (check-arg arg)
  (apply-ops/apply-op (assoc arg
                             :crdt group-store
                             :path (rest path)
                             :schema groups-schema)))

(defmethod apply-op :delete-value
  [{:keys [group-store path] :as arg}]
  (check-arg arg)
  (apply-ops/apply-op (assoc arg
                             :crdt group-store
                             :path (rest path)
                             :schema groups-schema)))

(defn apply-ops
  [{:keys [group-store ops sys-time-ms]
    :or {sys-time-ms (u/current-time-ms)}}]
  (reduce (fn [group-store* op]
            (apply-op (assoc op
                             :group-store group-store*
                             :sys-time-ms sys-time-ms)))
          group-store
          ops))

(defn get-value [{:keys [group-store path] :as arg}]
  (check-arg arg)
  (crdt-c/get-value (assoc arg
                           :crdt group-store
                           :path (rest path)
                           :schema groups-schema)))

(defn sub-group-ids
  [{:keys [group-id] :as arg}]
  (reduce-kv (fn [acc subject-id {:keys [type] :as info}]
               (if (= :group type)
                 (conj acc subject-id)
                 acc))
             []
             (get-value (assoc arg :path [:groups group-id]))))

(defn direct-member?
  "Does not include transitive members."
  [{:keys [group-id group-store subject-id] :as arg}]
  (or (= everyone-str group-id)
      (-> (get-value (assoc arg :path [:groups group-id subject-id]))
          (seq)
          (boolean))))

(defn member?
  "Includes transitive members."
  [{:keys [group-store group-id subject-id seen-group-ids]
    :or {seen-group-ids #{}}
    :as arg}]
  (when (seen-group-ids group-id)
    (throw (ex-info "Cycle exists in group membership." arg)))
  (or (direct-member? arg)
      (reduce (fn [acc group-id*]
                (if (member? (assoc arg
                                    :group-id group-id*
                                    :subject-id subject-id
                                    :seen-group-ids (conj seen-group-ids
                                                          group-id)))
                  (reduced true)
                  false))
              false
              (sub-group-ids arg))))
