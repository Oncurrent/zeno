(ns com.oncurrent.zeno.utils
  (:require
   [clojure.core.async :as ca]
   #?(:cljs [clojure.pprint :as pprint])
   [clojure.string :as str]
   [compact-uuids.core :as compact-uuid]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   #?(:clj [puget.printer :as puget])
   [taoensso.timbre :as log]
   [weavejester.dependency :as dep])
  #?(:cljs
     (:require-macros
      [com.oncurrent.zeno.utils :refer [sym-map go-log go-log-helper*]]))
  #?(:clj
     (:import
      (java.util UUID))

     :cljs
     (:import
      (goog.math Long))))

#?(:clj (set! *warn-on-reflection* true))

(def terminal-kw-ops #{:zeno/keys :zeno/count :zeno/concat})
(def kw-ops (conj terminal-kw-ops :zeno/*))
(def valid-path-roots #{:zeno/actor-id
                        :zeno/client
                        :zeno/crdt})

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn compact-random-uuid []
  (compact-uuid/str #?(:cljs (random-uuid)
                       :clj (UUID/randomUUID))))

(defn pprint [x]
  #?(:clj (let [^String s (puget/with-color (puget/pprint-str x))]
            (.write *out* s))
     :cljs (pprint/pprint x)))

(defn pprint-str [x]
  #?(:clj (puget/with-color (puget/pprint-str x))
     :cljs (with-out-str (pprint/pprint x))))

(defn int-pow [base exp]
  (int (Math/pow base exp)))

(defn str->int [s]
  (when (seq s)
    #?(:clj (Integer/parseInt s)
       :cljs (js/parseInt s))))

(defn str->long [s]
  (when (seq s)
    #?(:clj (Long/parseLong s)
       :cljs (.fromString ^Long Long s))))

(defn ex-msg [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(defn ex-stacktrace [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(defn ex-msg-and-stacktrace [e]
  (let [data (ex-data e)
        lines (cond-> [(str "\nException:\n" (ex-msg e))]
                data (conj (str "\nex-data:\n" (ex-data e)))
                true (conj (str "\nStacktrace:\n" (ex-stacktrace e))))]
    (str/join "\n" lines)))

(defn current-time-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.fromNumber ^Long Long (.getTime (js/Date.)))))

(defn monotonic-time-ms []
  #?(:clj (-> (System/nanoTime)
              (/ 1000000)
              (long))
     :cljs (int (js/performance.now))))

(defn long->str [l]
  #?(:clj (str l)
     :cljs (.toString ^Long l)))

(defmacro go-log-helper* [ex-type body]
  `(try
     ~@body
     (catch ~ex-type e#
       (log/error (ex-msg-and-stacktrace e#)))))

(defmacro go-log [& body]
  `(au/if-cljs
    (clojure.core.async/go
      (go-log-helper* :default ~body))
    (clojure.core.async/go
      (go-log-helper* Exception ~body))))

(defn round-int [n]
  (int #?(:clj (Math/round (float n))
          :cljs (js/Math.round n))))

(defn floor-int [n]
  (int #?(:clj (Math/round (Math/floor (float n)))
          :cljs (js/Math.floor n))))


;;;;;;;;;;;;;;;;;;;; Helper fns ;;;;;;;;;;;;;;;;;;;;

(defn check-sub-map
  [sub-map]
  (when-not (map? sub-map)
    (throw (ex-info
            (str "The `sub-map` argument must be a map. Got `" sub-map "`.")
            (sym-map sub-map))))
  (when-not (pos? (count sub-map))
    (throw (ex-info "The `sub-map` argument must contain at least one entry."
                    (sym-map sub-map))))
  (doseq [[sym path] sub-map]
    (when-not (symbol? sym)
      (throw (ex-info (str "Bad key `" sym "` in subscription map. Keys must "
                           "be symbols.")
                      {:bad-key sym
                       :sub-map sub-map})))
    (when (and (not (sequential? path))
               (not (symbol? path)))
      (throw (ex-info
              (str (str "Bad path. Paths must be sequences or symbols that "
                        "resolve to sequences. Got `" path "`."))
              (sym-map sym path sub-map))))))

(defn relationship-info
  "Given two key sequences, return a vector of [relationship tail].
   Relationship is one of :sibling, :parent, :child, or :equal.
   Tail is the keypath between the parent and child. Only defined when
   relationship is :parent."
  [ksa ksb]
  (let [va (vec ksa)
        vb (vec ksb)
        len-a (count va)
        len-b (count vb)
        len-min (min len-a len-b)
        divergence-i (loop [i 0]
                       (if (and (< i len-min)
                                (= (va i) (vb i)))
                         (recur (inc i))
                         i))
        a-tail? (> len-a divergence-i)
        b-tail? (> len-b divergence-i)]
    (cond
      (and a-tail? b-tail?) [:sibling nil]
      a-tail? [:child nil]
      b-tail? [:parent (drop divergence-i ksb)]
      :else [:equal nil])))

(defn has-join? [path]
  (some #(or (sequential? %)
             (set? %)
             (= :zeno/* %))
        path))

(defn empty-sequence-in-path? [path]
  (reduce (fn [acc element]
            (if (and (sequential? element)
                     (not (seq element)))
              (reduced true)
              acc))
          false path))

(defn cartesian-product [colls]
  (if (empty? colls)
    '(())
    (for [more (cartesian-product (rest colls))
          x (first colls)]
      (cons x more))))

(def initial-path-info {:template []
                        :in-progress []
                        :colls []
                        :coll-index 0})

(defn post-process-path-info [info]
  (let [{:keys [in-progress]} info]
    (cond-> info
      (seq in-progress) (update :template conj in-progress)
      true (select-keys [:template :colls]))))

(defn update-parse-path-acc [acc element]
  (let [{:keys [in-progress coll-index]} acc]
    (if-not (sequential? element)
      (update acc :in-progress conj element)
      (cond-> acc
        (seq in-progress) (update :template conj in-progress)
        (seq in-progress) (assoc :in-progress [])
        true (update :colls conj element)
        true (update :template conj coll-index)
        true (update :coll-index inc)))))

(defn parse-path [ks-at-path path]
  (let [info (reduce
              (fn [acc element]
                (let [element* (cond
                                 (set? element)
                                 (seq element)

                                 (= :zeno/* element)
                                 (ks-at-path (:in-progress acc))

                                 :else
                                 element)]
                  (update-parse-path-acc acc element*)))
              initial-path-info
              path)]
    (post-process-path-info info)))

(defn expand-template [template colls]
  (map (fn [combo]
         (vec (reduce (fn [acc element]
                        (concat acc (if (number? element)
                                      [(nth combo element)]
                                      element)))
                      '() template)))
       (cartesian-product colls)))

(defn expand-path [ks-at-path path]
  (if-not (has-join? path)
    [path]
    (let [{:keys [template colls]} (parse-path ks-at-path path)]
      (expand-template template colls))))

(defn throw-bad-path-root [path]
  (let [[head & tail] path
        disp-head (or head "nil")]
    (throw (ex-info (str "Paths must begin with one of " valid-path-roots
                         ". Got: `" disp-head "` in path `" path "`.")
                    (sym-map path head)))))

(defn throw-bad-path-key [path k]
  (throw (ex-info
          (str "Illegal key `" k "` in path `" path "`. Only integers, "
               "strings, keywords, symbols, maps, sequences and sets are "
               "valid path keys.")
          (sym-map k path))))

(defn check-key-types [path]
  (doseq [k path]
    (when (not (or (keyword? k)  (string? k) (int? k) (symbol? k) (nil? k)
                   (sequential? k) (set? k)))
      (throw-bad-path-key path k))))

(defn check-terminal-kws [path]
  (let [num-elements (count path)
        last-i (dec num-elements)]
    (doseq [i (range num-elements)]
      (let [k (nth path i)]
        (when (and (terminal-kw-ops k)
                   (not= last-i i))
          (throw (ex-info (str "`" k "` can only appear at the end of a path")
                          (sym-map path k))))))))

(defn check-path [path]
  (check-key-types path)
  (check-terminal-kws path))

(defn sub-map->map-info [sub-map resolution-map]
  (check-sub-map sub-map)
  (let [resolve-resolution-map-syms (fn [path]
                                      (reduce
                                       (fn [acc element]
                                         (if-not (symbol? element)
                                           (conj acc element)
                                           (if-let [v (get resolution-map
                                                           element)]
                                             (conj acc v)
                                             (conj acc element))))
                                       [] path))
        info (reduce-kv
              (fn [acc sym path]
                (when-not (symbol? sym)
                  (throw (ex-info
                          (str "All keys in sub-map must be symbols. Got `"
                               sym "`.")
                          (sym-map sym sub-map))))
                (let [path* (resolve-resolution-map-syms
                             (if (symbol? path)
                               (get resolution-map path)
                               path))
                      _ (check-path path*)
                      deps (filter symbol? path*)
                      add-deps (fn [init-g]
                                 (reduce (fn [g dep]
                                           (dep/depend g sym dep))
                                         init-g deps))]
                  (when-not (valid-path-roots (first path*))
                    (throw-bad-path-root path))
                  (cond-> acc
                    true (update :sym->path assoc sym path*)
                    true (update :g add-deps)
                    (empty? deps) (update :independent-syms conj sym))))
              {:g (dep/graph)
               :independent-syms #{}
               :sym->path {}}
              sub-map)
        {:keys [g independent-syms sym->path]} info
        i-sym->pair (fn [i-sym]
                      (let [path (->(sym->path i-sym))
                            [head & tail] path]
                        [i-sym path]))
        independent-pairs (mapv i-sym->pair independent-syms)
        ordered-dependent-pairs (reduce
                                 (fn [acc sym]
                                   (if (or (independent-syms sym)
                                           (not (sym->path sym)))
                                     acc
                                     (conj acc [sym (sym->path sym)])))
                                 [] (dep/topo-sort g))]
    (sym-map independent-pairs ordered-dependent-pairs)))

(defn check-config [{:keys [config config-type config-rules]}]
  (doseq [[k info] config-rules]
    (let [{:keys [required? checks]} info
          v (get config k)]
      (when (and required? (not v))
        (throw
         (ex-info
          (str "`" k "` is required, but is missing from the "
               (name config-type) " config map.")
          (sym-map k config))))
      (doseq [{:keys [pred msg]} checks]
        (when (and v pred (not (pred v)))
          (throw
           (ex-info
            (str "The value of `" k "` in the " (name config-type) " config "
                 "map is invalid. It " msg ". Got `" (or v "nil") "`.")
            (sym-map k v config))))))))
