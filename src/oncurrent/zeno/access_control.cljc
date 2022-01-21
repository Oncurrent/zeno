(ns oncurrent.zeno.access-control
  (:require
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt.apply-ops :as apply-ops]
   [oncurrent.zeno.crdt.common :as crdt-c]
   [oncurrent.zeno.groups :as groups]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def ac-store-schema
  (l/map-schema  ; key is the lancaster+b64 encode path
   (l/map-schema ; key is the subject-id
    schemas/permissions-schema)))

(def valid-permissions
  (-> schemas/permissions-schema l/edn :symbols set))

(defmulti apply-op :op-type)

(defn check-arg [{:keys [path value] :as arg}]
  (let [subject-id (last path)]
    (when-not (sequential? path)
      (throw (ex-info (str "Invalid path. Must be a sequence. Got `"
                           path "`.")
                      arg)))
    (when-not (string? subject-id)
      (throw (ex-info (str "Invalid subject-id. Must be a "
                           "string. Got `" subject-id "`.")
                      arg)))
    (when (and value
               (not (valid-permissions value)))
      (throw (ex-info (str "Invalid permissions. Must be one of `"
                           valid-permissions "`. Got `" value "`.")
                      arg)))))

(defn xf-path* [path]
  (->> path
       (l/serialize schemas/path-schema)
       (ba/byte-array->b64)))

(defn xf-path [path]
  (let [subject-id (last path)
        encoded-path (xf-path* (butlast path))]
    [encoded-path subject-id]))

(defmethod apply-op :add-value
  [{:keys [ac-store path] :as arg}]
  (check-arg arg)
  (apply-ops/apply-op (assoc arg
                             :crdt ac-store
                             :schema ac-store-schema)))

(defmethod apply-op :delete-value
  [{:keys [ac-store path] :as arg}]
  (check-arg arg)
  (apply-ops/apply-op (assoc arg
                             :crdt ac-store
                             :schema ac-store-schema)))

(defn apply-ops
  [{:keys [ac-store ops sys-time-ms]
    :or {sys-time-ms (u/current-time-ms)}}]
  (reduce (fn [ac-store* op]
            (apply-op (assoc op
                             :ac-store ac-store*
                             :sys-time-ms sys-time-ms)))
          ac-store
          ops))

(defn get-effective-acm
  [{:keys [ac-store path] :as arg}]
  (reduce (fn [acc i]
            (let [enc-path (->> path
                                (take (inc i))
                                (xf-path*))
                  acm (crdt-c/get-value (assoc arg
                                               :crdt ac-store
                                               :path [enc-path]
                                               :schema ac-store-schema))]
              (merge acc acm)))
          {groups/everyone-str :w}
          (range (count path))))

(defn can-read?
  [{:keys [subject-id] :as arg}]
  (check-arg arg)
  (let [acm (get-effective-acm arg)]
    (if (#{:r :rw} (acm subject-id))
      true
      (reduce-kv (fn [acc subject-id* perms]
                   (if (and (groups/member? (assoc arg :group-id subject-id*))
                            (#{:r :rw} perms))
                     (reduced true)
                     acc))
                 false
                 acm))))

(defn can-write?
  [{:keys [subject-id] :as arg}]
  (check-arg arg)
  (let [acm (get-effective-acm arg)]
    (if (#{:w :rw} (acm subject-id))
      true
      (reduce-kv (fn [acc subject-id* perms]
                   (if (and (groups/member? (assoc arg :group-id subject-id*))
                            (#{:w :rw} perms))
                     (reduced true)
                     acc))
                 false
                 acm))))
