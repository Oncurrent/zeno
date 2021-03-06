(ns com.oncurrent.zeno.client.state-subscriptions
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client.react.impl :as react-impl]
   [com.oncurrent.zeno.client.state-provider-impl :as sp-impl]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]
   [weavejester.dependency :as dep]))

(defn get-in-state [{root :root
                     zc :zc
                     :as arg}]
  (let [{:keys [root->state-provider]} zc
        state-provider (root->state-provider root)
        _ (when-not state-provider
            (throw (ex-info (str "No state provider found "
                                 "for root `" (or root "nil")
                                 "`.")
                            {:root root
                             :known-roots (keys root->state-provider)})))
        {::sp-impl/keys [get-in-state]} state-provider]
    (get-in-state arg)))

(defn get-non-numeric-part [path]
  (take-while #(not (number? %)) path))

(defn update-numeric? [updated-path sub-path]
  (let [u-front (get-non-numeric-part updated-path)
        s-front (get-non-numeric-part sub-path)]
    (or (empty? u-front)
        (empty? s-front)
        (let [[relationship _] (u/relationship-info u-front s-front)]
          (not= :sibling relationship)))))

(defn update-sub?* [updated-paths sub-path]
  (reduce (fn [acc updated-path]
            (cond
              (= [:zeno/actor-id] sub-path)
              (if (= [:zeno/actor-id] updated-path)
                (reduced true)
                false)

              (= [:zeno/actor-id] updated-path)
              (if (= [:zeno/actor-id] sub-path)
                (reduced true)
                false)

              (or (some number? updated-path)
                  (some number? sub-path))
              (if (update-numeric? updated-path sub-path)
                (reduced true)
                false)

              :else
              (let [[relationship _] (u/relationship-info
                                      (or updated-path [])
                                      (or sub-path []))]
                (if (= :sibling relationship)
                  false
                  (reduced true)))))
          false
          updated-paths))

(defn transform-operators-in-sub-path [sub-path]
  (reduce (fn [acc k]
            (if (u/kw-ops k)
              (reduced acc)
              (conj acc k)))
          [] sub-path))

(defn update-sub? [updated-paths sub-paths]
  (reduce
   (fn [acc sub-path]
     (when-not (sequential? sub-path)
       (throw (ex-info (str "`sub-path` must be seqential. Got: `"
                            (or sub-path "nil") "`.")
                       (u/sym-map sub-path))))
     (if (update-sub?* updated-paths
                       (transform-operators-in-sub-path sub-path))
       (reduced true)
       false))
   false
   sub-paths))

(defn get-state-sub-names-to-update [updated-paths *state-sub-name->info]
  (reduce-kv (fn [acc state-sub-name info]
               (let [{:keys [expanded-paths]} info]
                 (if (update-sub? updated-paths expanded-paths)
                   (conj acc state-sub-name)
                   acc)))
             #{}
             @*state-sub-name->info))

(defn order-by-lineage [state-sub-names-to-update *state-sub-name->info]
  (let [g (reduce
           (fn [acc state-sub-name]
             (let [{:keys [parents]} (@*state-sub-name->info state-sub-name)
                   update-parents (set/intersection parents
                                                    state-sub-names-to-update)]
               (if (empty? update-parents)
                 (dep/depend acc state-sub-name :zeno/root)
                 (reduce (fn [acc* parent]
                           (if (state-sub-names-to-update parent)
                             (dep/depend acc* state-sub-name parent)
                             acc*))
                         acc
                         update-parents))))
           (dep/graph)
           state-sub-names-to-update)]
    (->> (dep/topo-sort g)
         (filter #(not= :zeno/root %)))))

(defn ks-at-path [{:keys [full-path kw path root zc]}]
  (let [coll (:value (get-in-state (u/sym-map path root zc)))]
    (cond
      (map? coll)
      (keys coll)

      (sequential? coll)
      (range (count coll))

      (nil? coll)
      []

      :else
      (throw
       (ex-info
        (str "`" kw "` is in the path, but "
             "there is not a collection at " path ".")
        {:full-path full-path
         :missing-collection-path path
         :value coll})))))

(defn count-at-path [{:keys [path root zc]}]
  (let [coll (:value (get-in-state (u/sym-map path root zc)))]
    (cond
      (or (map? coll) (sequential? coll))
      (count coll)

      (nil? coll)
      0

      :else
      (throw
       (ex-info
        (str "`:zeno/count` terminates path, but there is not a collection at "
             path ".")
        {:path path
         :value coll})))))

(defn do-concat [{:keys [path root zc]}]
  (let [seqs (:value (get-in-state (u/sym-map path root zc)))]
    (when (and (not (nil? seqs))
               (or (not (sequential? seqs))
                   (not (sequential? (first seqs)))))
      (throw
       (ex-info
        (str "`:zeno/concat` terminates path, but there "
             "is not a sequence of sequences at " path ".")
        {:path path
         :value seqs})))
    (apply concat seqs)))

(defn get-value-and-expanded-paths [zc path root]
  ;; TODO: Optimize this. Only traverse the path once.
  (let [last-path-k (last path)
        join? (u/has-join? path)
        wildcard-parent (-> (partition-by #(= :zeno/* %) path)
                            (first))
        wildcard? (not= path wildcard-parent)
        terminal-kw? (u/terminal-kw-ops last-path-k)
        ks-at-path* #(ks-at-path {:full-path path
                                  :kw :zeno/*
                                  :path %
                                  :root root
                                  :zc zc})]
    (cond
      (u/empty-sequence-in-path? path)
      [nil [path]]

      (= [:zeno/actor-id] path)
      [@(:*actor-id zc) [path]]

      (and (not terminal-kw?) (not join?))
      (let [{:keys [norm-path value exists?]} (get-in-state
                                               (u/sym-map path root zc))]
        [value [norm-path] exists?])

      (and terminal-kw? (not join?))
      (let [path* (butlast path)
            value (case last-path-k
                    :zeno/keys
                    (ks-at-path {:full-path path
                                 :kw :zeno/keys
                                 :path path*
                                 :root root
                                 :zc zc})

                    :zeno/count
                    (count-at-path {:path path*
                                    :root root
                                    :zc zc})

                    :zeno/concat
                    (do-concat {:path path*
                                :root root
                                :zc zc}))]
        [value [path*]])

      (and (not terminal-kw?) join?)
      (let [xpaths (u/expand-path ks-at-path* path)
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          ;; Use loop to stay in go block
          (loop [out []
                 i 0]
            (let [path* (nth xpaths i)
                  [value _ exists?] (get-value-and-expanded-paths
                                     zc path* root)
                  new-out (if (false? exists?) out (conj out value))
                  new-i (inc i)]
              (if (not= num-results new-i)
                (recur new-out new-i)
                [new-out xpaths*])))))

      (and terminal-kw? join?)
      (let [xpaths (u/expand-path ks-at-path* (butlast path))
            num-results (count xpaths)
            xpaths* (if wildcard?
                      [wildcard-parent]
                      xpaths)]
        (if (zero? num-results)
          [[] xpaths*]
          (let [results (loop [out [] ;; Use loop to stay in go block
                               i 0]
                          (let [path* (nth xpaths i)
                                [value _ exists?] (get-value-and-expanded-paths
                                     zc path* root)
                                new-out (if (false? exists?) out (conj out value))
                                new-i (inc i)]
                            (if (not= num-results new-i)
                              (recur new-out new-i)
                              new-out)))
                v (case last-path-k
                    :zeno/keys (range (count results))
                    :zeno/count (count results)
                    :zeno/concat (apply concat results))]
            [v xpaths*]))))))

(defn resolve-symbols-in-path [{:keys [path state]}]
  (if (symbol? path)
    (get state path)
    (reduce (fn [acc element]
              (conj acc (if-not (symbol? element)
                          element
                          (get state element))))
            []
            path)))

(defn get-path-info [zc acc-state path resolve-path?]
  (let [{:keys [root->state-provider]} zc
        resolved-path (if resolve-path?
                        (resolve-symbols-in-path {:path path
                                                  :state acc-state})
                        path)
        [root & tail] resolved-path]
    (u/sym-map resolved-path root)))

(defn get-state-and-expanded-paths
  [zc independent-pairs ordered-dependent-pairs]
  (let [reducer* (fn [resolve-path? acc [sym path]]
                   (let [info (get-path-info zc (:state acc) path resolve-path?)
                         {:keys [resolved-path root]} info
                         [v xps] (get-value-and-expanded-paths
                                  zc resolved-path root)]
                     (-> acc
                         (update :state assoc sym v)
                         (update :expanded-paths concat xps))))
        init {:state {}
              :expanded-paths []}
        indep-ret (reduce (partial reducer* false) init independent-pairs)]
    (reduce (partial reducer* true) indep-ret ordered-dependent-pairs)))

(defn make-applied-update-fn [zc state-sub-name]
  (let [{:keys [*state-sub-name->info]} zc
        sub-info (@*state-sub-name->info state-sub-name)
        {:keys [independent-pairs ordered-dependent-pairs
                react? update-fn]} sub-info
        {:keys [state expanded-paths]} (get-state-and-expanded-paths
                                        zc
                                        independent-pairs
                                        ordered-dependent-pairs)]
    (when (and sub-info
               (not= (:state sub-info) state))
      {:react? react?
       :update-fn (fn []
                    (let [{:keys [update-fn]} sub-info
                          sub-info* (-> sub-info
                                        (assoc :state state)
                                        (assoc :expanded-paths expanded-paths))]
                      (swap! *state-sub-name->info
                             assoc state-sub-name sub-info*)
                      (update-fn state)))})))

(defn get-update-fn-info [zc state-sub-names]
  (reduce
   (fn [acc state-sub-name]
     (let [{:keys [update-fn react?]} (make-applied-update-fn
                                       zc state-sub-name)]
       (cond
         (not update-fn)
         acc

         react?
         (update acc :react-update-fns conj update-fn)

         :else
         (update acc :non-react-update-fns conj update-fn))))
   {:react-update-fns []
    :non-react-update-fns []}
   state-sub-names))

(defn update-subs! [state-sub-names zc]
  (let [update-fn-info (get-update-fn-info zc state-sub-names)
        {:keys [react-update-fns non-react-update-fns]} update-fn-info]
    (doseq [f non-react-update-fns]
      (f))
    (react-impl/batch-updates
     #(doseq [rf react-update-fns]
        (rf)))))

(defn do-subscription-updates! [{:keys [zc updated-paths]}]
  (let [{:keys [*state-sub-name->info]} zc
        subs-to-update (get-state-sub-names-to-update updated-paths
                                                      *state-sub-name->info)
        subs-by-lineage (order-by-lineage subs-to-update *state-sub-name->info)]
    (update-subs! subs-by-lineage zc)))

(defn subscribe-to-state!
  [zc state-sub-name sub-map update-fn opts]
  (when-not (string? state-sub-name)
    (throw (ex-info
            (str "The `state-sub-name` argument to `subscribe!` "
                 " must be a string. Got `" state-sub-name "`.")
            (u/sym-map state-sub-name sub-map opts))))
  (when-not @(:*stop? zc)
    (let [{:keys [valid-path-roots]} zc
          {:keys [react? resolution-map]} opts
          map-info (u/sub-map->map-info
                    (u/sym-map sub-map resolution-map valid-path-roots))
          {:keys [independent-pairs ordered-dependent-pairs]} map-info
          parents (set (:parents opts))]
      (let [sxps (get-state-and-expanded-paths
                  zc independent-pairs ordered-dependent-pairs)
            {:keys [expanded-paths]} sxps
            state (select-keys (:state sxps) (keys sub-map))
            info (u/sym-map independent-pairs ordered-dependent-pairs
                            expanded-paths parents react? update-fn state)]
        (swap! (:*state-sub-name->info zc) assoc state-sub-name info)
        state))))
