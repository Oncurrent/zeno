(ns com.oncurrent.zeno.utils
  (:require
   #?(:cljs [applied-science.js-interop :as j])
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
      (java.net URI)
      (java.net.http HttpClient
                     HttpRequest
                     HttpRequest$Builder
                     HttpResponse
                     HttpResponse$BodyHandler
                     HttpResponse$BodyHandlers)
      (java.util UUID)
      (java.util.concurrent CompletableFuture)
      (java.util.function BiFunction))

     :cljs
     (:import
      (goog.math Long))))

#?(:clj (set! *warn-on-reflection* true))

(def default-env-lifetime-mins 5)
(def default-env-name "main")
(def default-rpc-timeout-ms 30000)
(def terminal-kw-ops #{:zeno/keys :zeno/count :zeno/concat})
(def kw-ops (conj terminal-kw-ops :zeno/*))
;; TODO: Make valid-path-roots dynamic according to the client's
;; root->state-provider configuration. Then remove this variabl altogether and
;; use the new dynamic thing.
(def valid-path-roots #{:zeno/actor-id
                        :zeno/client
                        :zeno/crdt
                        :crdt
                        :local})

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

(defn pprint-str* [x]
  #?(:clj (puget/pprint-str x)
     :cljs (with-out-str (pprint/pprint x))))

(defn pprint-str [x]
  #?(:clj (puget/with-color (pprint-str* x))
     :cljs (pprint-str* x)))

(defn int-pow [base exp]
  (int (Math/pow base exp)))

(defn str->int [s]
  (when (seq s)
    #?(:clj (Integer/parseInt s)
       :cljs (js/parseInt s 10))))

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

(defn <parse-path [<ks-at-path path]
  (au/go
    (let [path-len (count path)
          ;; Use loop to stay in go block
          info (loop [acc initial-path-info
                      i 0]
                 (let [element (nth path i)
                       element* (cond
                                  (set? element)
                                  (seq element)
                                  (= :zeno/* element)
                                  (au/<? (<ks-at-path (:in-progress acc)))
                                  :else
                                  element)
                       new-acc (update-parse-path-acc acc element*)
                       new-i (inc i)]
                   (if (= path-len new-i)
                     new-acc
                     (recur new-acc new-i))))]
      (post-process-path-info info))))

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

(defn <expand-path [<ks-at-path path]
  (au/go
   (if-not (has-join? path)
     [path]
     (let [{:keys [template colls]} (au/<? (<parse-path <ks-at-path path))]
       (expand-template template colls)))))

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

(defn get-normalized-array-index
  "Translates relative indexing (e.g. using negative numbers to index from the
   end) into absolute indexing. Returns nil if out of bounds."
  [{:keys [array-len i]}]
  (let [max-i (if (pos? array-len)
                (dec array-len)
                0)
        norm-i (if (nat-int? i)
                 i
                 (+ array-len i))]
    (cond
      (neg? norm-i) nil
      (> norm-i max-i) nil
      :else norm-i)))

(defn get-clamped-array-index
  "Translates relative indexing (e.g. using negative numbers to index from the
   end) into absolute indexing. Returns the first or last element if out of
   bounds (depending on which end it's out)."
  [{:keys [array-len i]}]
  (let [max-i (if (pos? array-len)
                (dec array-len)
                0)
        norm-i (if (nat-int? i)
                 i
                 (+ array-len i))]
    (cond
      (neg? norm-i) 0
      (> norm-i max-i) max-i
      :else norm-i)))

(defn map->query-string [{:keys [ks m]}]
  (let [kvs (reduce (fn [acc k]
                      (let [v (get m k)]
                        (if (nil? v)
                          acc
                          (conj acc (str (name k) "=" v)))))
                    []
                    ks)]
    (str/join "&" kvs)))

(defn query-string->map [s]
  (when (not-empty s)
    (let [parts (str/split s #"&")]
      (reduce (fn [acc part]
                (let [[k v] (str/split part #"=")]
                  (assoc acc (keyword k) v)))
              {}
              parts))))

(defn chop-root [path root]
  (if (or (empty? path) (not= root (first path)))
    path
    (recur (rest path) root)))

(defn start-task-loop!
  "Runs a task function periodically in a loop.
   Takes a map with these keys:
     - `:loop-delay-ms` - Optional - Number of ms to wait between
                          task fn invocations. Defaults to 1000 ms.
     - `:loop-name` - Optional - A string name for this loop; used
                      in error logging.
     - `:task-fn` - Required - The fn to be called in each loop iteration.
    Returns a map with these keys:
     - `:now!` - A fn that can be called to cut short the loop
                 delay and call the task-fn immediately. Any args passed
                 into the `:now!` fn will be passed to the `:task-fn`.
     - `:stop!` - A zero-arg fn that can be called to stop the loop."
  [{:keys [loop-delay-ms loop-name task-fn] :as arg}]
  (when-not (ifn? task-fn)
    (throw (ex-info (str "`:task-fn` must be a function. Got `"
                         (or task-fn "nil") "`.")
                    arg)))
  (let [stop-ch (ca/promise-chan)
        now-ch (ca/chan (ca/dropping-buffer 1))
        stop! #(ca/put! stop-ch true)
        now! (fn [& args]
               #(ca/put! now-ch args))
        delay-ms (or loop-delay-ms 1000)]
    (ca/go
      (loop [args []]
        (try
          (let [ret (apply task-fn args)]
            (when (au/channel? ret)
              (au/<? ret)))
          (catch #?(:clj Exception :cljs js/Error) e
            (log/error (str "Error in task loop"
                            (if loop-name
                              (str " `" loop-name "`:")
                              ":")
                            (ex-msg-and-stacktrace e)))))
        (let [timeout-ch (ca/timeout delay-ms)
              [v ch] (au/alts? [stop-ch now-ch timeout-ch] :priority true)]
          (condp = ch
            stop-ch nil
            timeout-ch (recur [])
            now-ch (recur v)))))
    (sym-map now! stop!)))

(defn fill-env-defaults [m]
  (-> m
      (update :zeno/env-name #(cond
                               (not-empty %) %
                               (or (not-empty (:zeno/source-env-name m))
                                   (:zeno/env-lifetime-mins m))
                               (compact-random-uuid)
                               :else default-env-name))
      (update :zeno/source-env-name #(if (not-empty %) % default-env-name))
      (update :zeno/env-lifetime-mins #(or % default-env-lifetime-mins))))

(defn env-params->query-string [m]
  (map->query-string {:ks [:env-name :source-env-name :env-lifetime-mins]
                      :m m}))

(defn query-string->env-params [s]
  (-> (query-string->map s)
      (update :env-lifetime-mins str->int)))

(defn <http-get [{:keys [url]}]
  (when-not (string? url)
    (throw (ex-info (str "The value of the `:url` key must be a string. Got `"
                         (or url "nil") "`.")
                    (sym-map url))))
  #?(:clj
     (let [rsp-ch (ca/chan)
           client ^HttpClient (HttpClient/newHttpClient)
           req (.build ^HttpRequest$Builder (HttpRequest/newBuilder
                                             (URI. url)))
           body-handler (HttpResponse$BodyHandlers/ofByteArray)
           fut ^CompletableFuture (.sendAsync
                                   client
                                   req
                                   ^HttpResponse$BodyHandler body-handler)
           f ^BiFunction (reify BiFunction
                           (apply [this rsp e]
                             (let [status-code (.statusCode ^HttpResponse rsp)
                                   ret (cond
                                         e
                                         e

                                         (= 200 status-code)
                                         (.body ^HttpResponse rsp)

                                         :else
                                         (ex-info
                                          (str "Request failed. Status code: "
                                               status-code)
                                          (sym-map status-code url)))]
                               (ca/put! rsp-ch ret))))]
       (.handle fut f)
       rsp-ch)

     :cljs
     (let [ret-ch (ca/chan)]
       (-> (js/fetch url)
           (.then (fn [rsp]
                    (let [status-code (j/get rsp :status)]
                      (if-not (= 200 status-code)
                        (ca/put! ret-ch (ex-info
                                         (str "Request failed. Status code: "
                                              status-code)
                                         (sym-map status-code url)))
                        (-> (j/call rsp :arrayBuffer)
                            (.then (fn [ab]
                                     (ca/put! ret-ch (js/Int8Array. ab)))))))))
           (.catch (fn [e]
                     (ca/put! ret-ch e))))
       ret-ch)))

;;;;;;;;;;;;;;;;;;;; Platform detection ;;;;;;;;;;;;;;;;;;;;

(defn jvm? []
  #?(:clj true
     :cljs false))

(defn browser? []
  #?(:clj false
     :cljs (exists? js/navigator)))

(defn node? []
  #?(:clj false
     :cljs (boolean (= "nodejs" cljs.core/*target*))))
