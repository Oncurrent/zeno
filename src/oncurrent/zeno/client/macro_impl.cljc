(ns oncurrent.zeno.client.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.walk :as walk]
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(defn check-arglist [component-name subscription? arglist]
  (when-not (vector? arglist)
    (throw
     (ex-info (str "Illegal argument list in component `" component-name
                   "`. The argument list must be a vector. Got: `" arglist
                   "` which is a " (type arglist) ".")
              (u/sym-map arglist component-name))))
  (let [first-arg (first arglist)]
    (when (and subscription?
               (or (nil? first-arg)
                   (not= "zc" (when (symbol? first-arg)
                                (name first-arg)))))
      (throw
       (ex-info (str "Bad constructor arglist for component `" component-name
                     "`. First argument must be `zc`"
                     " (the zeno client). Got: `" first-arg "`.")
                (u/sym-map component-name first-arg arglist))))))

;; TODO: Improve error msgs for incorrect args (wrong num args, etc.)
(defn parse-def-component-args [component-name args]
  (let [parts (if (string? (nth args 0))
                (if (map? (nth args 2))
                  (let [[docstring arglist sub-map & body] args]
                    (u/sym-map docstring arglist sub-map body))
                  (let [[docstring arglist & body] args]
                    (u/sym-map docstring arglist body)))
                (if (map? (nth args 1))
                  (let [[arglist sub-map & body] args]
                    (u/sym-map arglist sub-map body))
                  (let [[arglist & body] args]
                    (u/sym-map arglist body))))
        {:keys [docstring sub-map arglist body]} parts
        _ (check-arglist component-name (boolean sub-map) arglist)
        _ (when sub-map
            (u/check-sub-map sub-map))
        repeated-syms (vec (set/intersection (set (keys sub-map))
                                             (set arglist)))]
    (when (seq repeated-syms)
      (throw
       (ex-info (str "Illegal repeated symbol(s) in component "
                     component-name "`. The same symbol may not appear in "
                     "both the subscription map and the argument list. "
                     "Repeated symbols: " repeated-syms)
                (u/sym-map repeated-syms sub-map arglist component-name))))
    parts))

(defn destructure*
  ;; Work around fact that clojure.core/destructure returns code that
  ;; is not cljs-compatible
  [bindings]
  #?(:clj
     (walk/postwalk-replace
      {'clojure.lang.PersistentHashMap/create 'hash-map}
      (destructure bindings))))

(defn next-instance-num! [zc]
  (swap! (:*next-instance-num zc) inc))

(defn make-outer-body [sub-map props-sym fq-name inner-component-name]
  `(let [body-fn# (oncurrent.zeno.client.react/get* ~props-sym "body-fn")
         zc# (oncurrent.zeno.client.react/get* ~props-sym "zc")
         sub-map# (oncurrent.zeno.client.react/get* ~props-sym "sub-map")
         resolution-map# (oncurrent.zeno.client.react/get* ~props-sym
                                                           "resolution-map")
         [instance-name# _#] (oncurrent.zeno.client.react/use-state
                              (str ~fq-name "-" (next-instance-num! zc#)))
         parents# (oncurrent.zeno.client.react/get* ~props-sym "parents")
         zeno-state# (oncurrent.zeno.client.react/use-zeno-state
                      zc# sub-map# instance-name# resolution-map# parents#)
         inner-props# (oncurrent.zeno.client.react/js-obj*
                       ["body-fn" body-fn#
                        "zeno-state" zeno-state#
                        "parents" (conj parents# instance-name#)])]
     (when (and zeno-state# (not= :zeno/unknown zeno-state#))
       (oncurrent.zeno.client.react/create-element
        ~inner-component-name inner-props#))))

(defn make-inner-body [props-sym fq-name]
  `(let [body-fn# (oncurrent.zeno.client.react/get* ~props-sym "body-fn")
         zeno-state# (oncurrent.zeno.client.react/get* ~props-sym "zeno-state")
         parents# (oncurrent.zeno.client.react/get* ~props-sym "parents")]
     (binding [oncurrent.zeno.client.react/*parents* parents#]
       (body-fn# zeno-state#))))

(defn get-resolution-map-keys [sub-map]
  (let [sub-map-ks (set (keys sub-map))]
    (-> (reduce-kv (fn [acc k path]
                     (if (symbol? path)
                       (conj acc path)
                       (reduce (fn [acc* element]
                                 (if (or (not (symbol? element))
                                         (sub-map-ks element))
                                   acc*
                                   (conj acc* element)))
                               acc path)))
                   #{} sub-map)
        (vec))))

(defn build-component
  ([ns-name component-name args]
   (build-component ns-name component-name args nil))
  ([ns-name component-name args dispatch-val]
   (let [fq-name (str ns-name "/" component-name)
         parts (parse-def-component-args fq-name args)
         {:keys [docstring arglist sub-map body]} parts
         first-line (if dispatch-val
                      `(defmethod ~component-name ~dispatch-val)
                      (cond-> (vec `(defn ~component-name))
                        docstring (conj docstring)))
         body-fn-name (symbol (str (name component-name) "-body-fn"))
         inner-component-name (gensym (str (name component-name) "-inner"))
         outer-component-name (gensym (str (name component-name) "-outer"))
         props-sym (gensym "props")
         arglist-raw-sym (gensym "arglist-raw")
         destructured (destructure* [arglist arglist-raw-sym])
         inner-component-form `(defn ~inner-component-name [~props-sym]
                                 ~(make-inner-body props-sym fq-name))
         outer-component-form `(defn ~outer-component-name [~props-sym]
                                 ~(make-outer-body sub-map props-sym fq-name
                                                   inner-component-name))
         res-map-ks (get-resolution-map-keys sub-map)
         sub-map-ks (keys sub-map)
         zc-sym (if sub-map
                  `~'zc
                  `nil)
         c-form `(~@first-line
                  [& ~arglist-raw-sym]
                  (let [~@destructured
                        resolution-map# (zipmap (quote ~res-map-ks)
                                                ~res-map-ks)
                        body-fn# (fn ~body-fn-name [zeno-state#]
                                   (let [{:syms [~@sub-map-ks]} zeno-state#]
                                     ~@body))

                        props# (oncurrent.zeno.client.react/js-obj*
                                ["body-fn" body-fn#
                                 "zc" ~zc-sym
                                 "sub-map" '~sub-map
                                 "resolution-map" resolution-map#
                                 "parents" oncurrent.zeno.client.react/*parents*])]
                    (oncurrent.zeno.client.react/create-element
                     ~(if sub-map
                        outer-component-name
                        inner-component-name)
                     props#)))
         forms (if sub-map
                 [inner-component-form outer-component-form c-form]
                 [inner-component-form c-form])]
     `(do
        ~@forms))))
