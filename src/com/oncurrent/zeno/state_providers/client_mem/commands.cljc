(ns com.oncurrent.zeno.state-providers.client-mem.commands
  (:require
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.schemas :as schemas]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(def insert-ops #{:zeno/insert-after
                  :zeno/insert-before
                  :zeno/insert-range-after
                  :zeno/insert-range-before})

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn normalize-neg-k
  "Return the normalized key and the associated value or nil if key does not
   exist in value."
  [k v]
  (if (map? v)
    [k (v k)]
    (let [len (count v)
          norm-k (+ len k)]
      [norm-k (when (and (pos? len) (nat-int? norm-k) (< norm-k len))
                (v norm-k))])))

(defn get-in-state
  "Custom get-in fn that checks types and normalizes negative keys.
   Returns a map with :norm-path and :value keys."
  ([state path]
   (get-in-state state path nil))
  ([state path roots*]
   (let [roots (cond
                    (nil? roots*) #{}
                    (set? roots*) roots*
                    :else (set [roots*]))
         [path-head & path-tail] path]
     (when (and (seq roots)
                (not (roots path-head)))
       (throw (ex-info (str "Illegal path. Path must start with one of "
                            roots ". Got `" path "`.")
                       (u/sym-map path roots path-head))))
     (reduce (fn [{:keys [value] :as acc} k]
               (let [[k* value*] (cond
                                   (or (keyword? k) (nat-int? k) (string? k))
                                   [k (when value
                                        (let [x (get value k)]
                                          ;; Treat empty colls as nil to align
                                          ;; with CRDT behavior
                                          (when (or (not (coll? x))
                                                    (seq x))
                                            x)))]

                                   (and (int? k) (neg? k))
                                   (normalize-neg-k k value)

                                   (nil? k)
                                   [nil nil]

                                   :else
                                   (throw-bad-path-key path k))]
                 (-> acc
                     (update :norm-path conj k*)
                     (assoc :value value*))))
             {:norm-path (if (seq roots)
                           [path-head]
                           [])
              :value state}
             (if (seq roots)
               path-tail
               path)))))

(defmulti eval-cmd (fn [state {:zeno/keys [op]} root]
                     (if (insert-ops op)
                       :zeno/insert*
                       op)))

(defmethod eval-cmd :zeno/set
  [state {:zeno/keys [path op arg]} root]
  (let [{:keys [norm-path]} (get-in-state state path root)
        state-path (if root
                     (rest norm-path)
                     norm-path)
        new-state (if (seq state-path)
                    (assoc-in state state-path arg)
                    arg)]
    {:state new-state
     :update-info {:norm-path norm-path
                   :op op
                   :value arg}}))

(defmethod eval-cmd :zeno/remove
  [state {:zeno/keys [path op]} root]
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path value]} (get-in-state state parent-path root)
        [new-parent path-k] (cond
                              (nil? value)
                              [nil k]

                              (map? value)
                              [(dissoc value k) k]

                              :else
                              (let [norm-i (u/get-normalized-array-index
                                            {:array-len (count value)
                                             :i k})
                                    [h t] (split-at norm-i value)]
                                (if (nat-int? norm-i)
                                  [(vec (concat h (rest t))) norm-i]
                                  (throw
                                   (ex-info
                                    (str "Index `" norm-i "` into array `"
                                         value "` is out of bounds.")
                                    (u/sym-map norm-i path
                                               norm-path))))))
        state-path (if root
                     (rest norm-path)
                     norm-path)
        new-state (cond
                    (nil? new-parent)
                    state

                    (empty? state-path)
                    new-parent

                    :else
                    (assoc-in state state-path new-parent))]
    {:state new-state
     :update-info {:norm-path (conj norm-path path-k)
                   :op op
                   :value nil}}))

(defmethod eval-cmd :zeno/insert*
  [state {:zeno/keys [arg op path]} root]
  (let [parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map parent-path i path op arg))))
        {:keys [norm-path value]} (get-in-state state parent-path root)
        _ (when-not (or (sequential? value) (nil? value))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a sequence. Got: `" value "`.")
                            (u/sym-map op path value norm-path))))
        range? (#{:zeno/insert-range-before :zeno/insert-range-after} op)
        _ (when (and range?
                     (not (sequential? arg)))
            (throw (ex-info (str "In `" op "` operations, the `:zeno/arg` "
                                 "value must be sequential. Got `" arg "`.")
                            (u/sym-map op path value norm-path))))
        clamped-i (u/get-clamped-array-index {:array-len (count value)
                                              :i i})
        split-i (if (#{:zeno/insert-before :zeno/insert-range-before} op)
                  clamped-i
                  (inc clamped-i))
        [h t] (split-at split-i value)
        new-parent (vec (concat h (if range? arg [arg]) t))
        state-path (if root
                     (rest norm-path)
                     norm-path)
        new-state (if (empty? state-path)
                    new-parent
                    (assoc-in state state-path new-parent))]
    {:state new-state
     :update-info {:norm-path (conj norm-path split-i)
                   :op op
                   :value arg}}))

(defn eval-cmds [initial-state cmds root]
  (reduce (fn [{:keys [state] :as acc} cmd]
            (let [ret (eval-cmd state cmd root)]
              (-> acc
                  (assoc :state (:state ret))
                  (update :update-infos conj (:update-info ret)))))
          {:state initial-state
           :update-infos []}
          cmds))
