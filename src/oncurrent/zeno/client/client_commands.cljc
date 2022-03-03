(ns oncurrent.zeno.client.client-commands
  (:require
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

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
  ([state path prefixes*]
   (let [prefixes (cond
                    (nil? prefixes*) #{}
                    (set? prefixes*) prefixes*
                    :else (set [prefixes*]))
         [path-head & path-tail] path]
     (when (and (seq prefixes)
                (not (prefixes path-head)))
       (throw (ex-info (str "Illegal path. Path must start with one of "
                            prefixes ". Got `" path "`.")
                       (u/sym-map path prefixes path-head))))
     (reduce (fn [{:keys [value] :as acc} k]
               (let [[k* value*] (cond
                                 (or (keyword? k) (nat-int? k) (string? k))
                                 [k (when value
                                      (get value k))]

                                 (and (int? k) (neg? k))
                                 (normalize-neg-k k value)

                                 (nil? k)
                                 [nil nil]

                                 :else
                                 (throw-bad-path-key path k))]
                 (-> acc
                     (update :norm-path conj k*)
                     (assoc :value value*))))
             {:norm-path (if (seq prefixes)
                           [path-head]
                           [])
              :value state}
             (if (seq prefixes)
               path-tail
               path)))))

(defmulti eval-cmd (fn [state {:zeno/keys [op]} prefix]
                     op))

(defmethod eval-cmd :zeno/set
  [state {:zeno/keys [path op arg]} prefix]
  (let [{:keys [norm-path]} (get-in-state state path prefix)
        state-path (if prefix
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
  [state {:zeno/keys [path op]} prefix]
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path value]} (get-in-state state parent-path prefix)
        [new-parent path-k] (cond
                              (nil? value)
                              [nil k]

                              (map? value)
                              [(dissoc value k) k]

                              :else
                              (let [norm-i (if (nat-int? k)
                                             k
                                             (+ (count value) k))
                                    [h t] (split-at norm-i value)]
                                (if (nat-int? norm-i)
                                  [(vec (concat h (rest t))) norm-i]
                                  (throw (ex-info "Path index out of range."
                                                  (u/sym-map norm-i path
                                                             norm-path))))))
        state-path (if prefix
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

(defn insert* [state path prefix op arg]
  (let [parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map parent-path i path op arg))))
        {:keys [norm-path value]} (get-in-state state parent-path prefix)
        _ (when-not (or (sequential? value) (nil? value))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a sequence. Got: `" value "`.")
                            (u/sym-map op path value norm-path))))
        norm-i (if (nat-int? i)
                 i
                 (+ (count value) i))
        ;; TODO: Check for out of range norm-i
        split-i (if (= :zeno/insert-before op)
                  norm-i
                  (inc norm-i))
        [h t] (split-at split-i value)
        new-t (cons arg t)
        new-parent (vec (concat h new-t))
        state-path (if prefix
                     (rest norm-path)
                     norm-path)
        new-state (if (empty? state-path)
                    new-parent
                    (assoc-in state state-path new-parent))]
    {:state new-state
     :update-info {:norm-path (conj norm-path split-i)
                   :op op
                   :value arg}}))

(defmethod eval-cmd :zeno/insert-before
  [state {:zeno/keys [path op arg]} prefix]
  (insert* state path prefix op arg))

(defmethod eval-cmd :zeno/insert-after
  [state {:zeno/keys [path op arg]} prefix]
  (insert* state path prefix op arg))

(defn eval-cmds [initial-state cmds prefix]
  (reduce (fn [{:keys [state] :as acc} cmd]
            (let [ret (eval-cmd state cmd prefix)]
              (-> acc
                  (assoc :state (:state ret))
                  (update :update-infos conj (:update-info ret)))))
          {:state initial-state
           :update-infos []}
          cmds))
