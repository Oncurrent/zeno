(ns oncurrent.zeno.sharing
  (:require
   [clojure.set :as set]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.commands :as crdt-commands]
   [oncurrent.zeno.crdt.common :as crdt-c]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def all-permissions
  (-> (l/edn schemas/sharing-group-member-permissions-schema)
      (:symbols)
      (set)))

(defmulti xf-cmd :op)

(defn throw-unknown-sg-field [path]
  (throw (ex-info (str "Unknown field `" (last path)
                       "` in Sharing Group path.")
                  {:path path})))

(defn check-member-id [member-id]
  (when-not (string? member-id)
    (throw (ex-info (str "Member id must be a string. Got `" member-id "`.")
                    (u/sym-map member-id)))))

(defn check-members-map [members]
  (when-not (associative? members)
    (throw (ex-info (str "Value of the `:zeno/members` key must be "
                         "associative (usually a map). Got `"  members "`.")
                    (u/sym-map members)))))

(defn check-member-info-map [member-info]
  (when-not (associative? member-info)
    (throw (ex-info (str "Value in a member permissions map must be "
                         "associative (usually a map). Got `"  member-info "`.")
                    (u/sym-map member-info)))))

(defn check-sg-id [sharing-group-id]
  (when-not (string? sharing-group-id)
    (throw (ex-info (str "Sharing Group id must be a string. Got `"
                         sharing-group-id "`.")
                    (u/sym-map sharing-group-id)))))

(defn check-sg-path [path]
  (when-not (sequential? path)
    (throw (ex-info (str "Path must be a sequence. Got `" path "`.")
                    {:path path}))))

(defn throw-set-membership-status [path]
  (throw (ex-info (str "Can't set `:zeno/membership-status` directly. "
                       "Zeno manages this field.")
                  {:path path})))

(defn throw-set-path-value [path]
  (throw (ex-info "Can't set value of a path"
                  {:path path})))

(defn dispatch-members-path [path]
  (let [[sg-id sg-k member-id member-k] path]
    (check-member-id member-id)
    (case member-k
      :zeno/permissions :set-sg-member-permissions
      :zeno/membership-status (throw-set-membership-status path)
      (throw-unknown-sg-field path))))

(defn dispatch-members-perm [path]
  (let [[sg-id sg-k member-id member-k permission] path]
    (check-member-id member-id)
    (if (all-permissions permission)
      :set-sg-member-permission
      (throw (ex-info (str "Unknown Sharing Group member permission `"
                           permission "`.")
                      (u/sym-map path permission))))))

(defn xf-set-cmd-dispatch [{:keys [path]}]
  (let [num-items (count path)
        sg-k (second path)]
    (if (zero? num-items)
      :set-root
      (do
        (check-sg-id (first path))
        (case num-items
          1 :set-sg
          2 (case sg-k
              :zeno/paths :set-sg-paths
              :zeno/members :set-sg-members
              (throw-unknown-sg-field path))
          3 (case sg-k
              :zeno/paths (do (check-sg-path (last path))
                              :set-sg-path)
              :zeno/members (do (check-member-id (last path))
                                :set-sg-member-info)
              (throw-unknown-sg-field path))
          4 (case sg-k
              :zeno/paths (throw-set-path-value path)
              :zeno/members (dispatch-members-path path)
              (throw-unknown-sg-field path))
          5 (case sg-k
              :zeno/paths (throw-set-path-value path)
              :zeno/members (dispatch-members-perm path)
              (throw-unknown-sg-field path)))))))

(defmulti xf-set-cmd xf-set-cmd-dispatch)

(defn permission->str [permission]
  (->> permission
       (l/serialize schemas/sharing-group-member-permissions-schema)
       (ba/byte-array->b64)))

(defn xf-permission [permission]
  (when-not (all-permissions permission)
    (throw (ex-info (str "Unknown Sharing Group member "
                         "permission `" permission "`.")
                    (u/sym-map permission))))
  (permission->str permission))

(defn xf-permissions [permissions]
  (when-not (set? permissions)
    (throw (ex-info (str "Sharing group member permissions must be a set. Got `"
                         permissions "`.")
                    (u/sym-map permissions))))
  (reduce (fn [acc permission]
            (assoc acc (xf-permission permission) true))
          {}
          permissions))

(defn xf-member [member-info]
  (check-member-info-map member-info)
  (let [{:zeno/keys [membership-status]} member-info]
    (when (and membership-status (not= :invited membership-status))
      (throw (ex-info (str "Can't set `:zeno/membership-status` directly. "
                           "Zeno manages this field.")
                      (u/sym-map member-info))))
    (update member-info :zeno/permissions xf-permissions)))

(defn xf-members [members]
  (when members
    (check-members-map members)
    (reduce-kv (fn [acc member-id m]
                 (check-member-id member-id)
                 (assoc acc member-id (xf-member m)))
               {}
               members)))

(defn path->str [path]
  (->> path
       (l/serialize schemas/path-schema)
       (ba/byte-array->b64)))

(defn xf-paths [paths]
  (when paths
    (when-not (set? paths)
      (throw (ex-info (str "Paths must be a set. Got `" paths "`.")
                      {:paths paths})))
    (reduce (fn [acc path]
              (assoc acc (path->str path) true))
            {}
            paths)))

(defn xf-sg-arg [arg]
  (-> arg
      (update :zeno/members xf-members)
      (update :zeno/paths xf-paths)))

(defn xf-root-arg [arg]
  (reduce-kv
   (fn [acc sg-id m]
     (check-sg-id sg-id)
     (assoc acc sg-id (xf-sg-arg m)))
   {}
   arg))

(defmethod xf-set-cmd :set-root
  [cmd]
  (update cmd :arg xf-root-arg))

(defmethod xf-set-cmd :set-sg
  [cmd]
  (update cmd :arg xf-sg-arg))

(defmethod xf-set-cmd :set-sg-paths
  [cmd]
  (update cmd :arg xf-paths))

(defmethod xf-set-cmd :set-sg-members
  [cmd]
  (update cmd :arg xf-members))

(defmethod xf-set-cmd :set-sg-member-info
  [cmd]
  (update cmd :arg xf-member))

(defmethod xf-set-cmd :set-sg-member-permissions
  [cmd]
  (update cmd :arg xf-permissions))

(defmethod xf-set-cmd :set-sg-member-permission
  [cmd]
  (-> cmd
      (update :arg boolean)
      (update :path (fn [old-path]
                      (concat (butlast old-path)
                              [(permission->str (last old-path))])))))

(defmethod xf-cmd :set
  [arg]
  (xf-set-cmd arg))

(defn xf-arg [arg]
  (-> arg
      (update-in [:cmd :path] rest)
      (update-in [:cmd] xf-cmd)
      (assoc :crdt (:sharing-store arg))
      (assoc :schema schemas/sharing-store-schema)))

(defn process-cmd [arg]
  (let [arg* (xf-arg arg)
        {:keys [crdt ops]} (crdt-commands/process-cmd arg*)]
    (-> arg
        (assoc :sharing-store crdt)
        (update :ops set/union ops))))

(defn make-path-str->sg-ids [sharing-store]
  (reduce-kv
   (fn [acc sg-id {:zeno/keys [members paths]}]
     (let [paths-set (reduce-kv (fn [acc path present?]
                                  (if present?
                                    (conj acc path)
                                    acc))
                                #{}
                                paths)]
       (reduce (fn [acc* path]
                 (update acc path (fn [existing]
                                    (conj (or existing #{}) sg-id))))
               acc
               paths-set)))
   {}
   (crdt/get-value {:crdt sharing-store
                    :path []
                    :schema schemas/sharing-store-schema})))

(defn member-has-permission? [{:keys [sharing-store
                                      sg-id
                                      member-id
                                      permission]
                               :as arg}]
  (let [sg (crdt/get-value {:crdt sharing-store
                            :path [sg-id]
                            :schema schemas/sharing-store-schema})]
    (get-in sg [:zeno/members member-id :zeno/permissions
                (permission->str permission)])))

(defn sgs-allow? [{:keys [actor-id sg-ids]
                   :as arg}]
  (reduce (fn [acc sg-id]
            (if (member-has-permission? (assoc arg
                                               :member-id actor-id
                                               :sg-id sg-id))
              (reduced true)
              acc))
          false
          sg-ids))

(defn allowed? [{:keys [path sharing-store] :as arg}]
  (let [path-str->sg-ids (make-path-str->sg-ids sharing-store)
        ret (reduce (fn [acc n]
                      (let [path* (take n path)
                            sg-ids (path-str->sg-ids (path->str path*))]
                        (if (empty? sg-ids)
                          acc ; Check parent
                          (reduced (sgs-allow?
                                    (assoc arg
                                           :path path*
                                           :sg-ids sg-ids))))))
                    :root
                    (-> (count path)
                        (inc)
                        (range)
                        (reverse)))]
    (if (= :root ret)
      true ; No sharing in path. It's private and R/W by actor.
      ret  ; Sharing found, use the permissions of the sharing group
      )))

(defn can-write? [arg]
  (allowed? (assoc arg :permission :zeno/write-data)))

(defn can-read? [arg]
  (allowed? (assoc arg :permission :zeno/read-data)))
