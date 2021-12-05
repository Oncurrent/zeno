(ns  oncurrent.zeno.utils
  (:require
   [clojure.core.async :as ca]
   #?(:cljs [clojure.pprint :as pprint])
   [clojure.string :as str]
   [compact-uuids.core :as compact-uuid]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   #?(:clj [puget.printer :as puget])
   [taoensso.timbre :as log])
  #?(:cljs
     (:require-macros
      [oncurrent.zeno.utils :refer [sym-map go-log go-log-helper*]]))
  #?(:clj
     (:import
      (java.util UUID))

     :cljs
     (:import
      (goog.math Long))))

#?(:clj (set! *warn-on-reflection* true))

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
  #?(:clj (.write *out* ^String (puget/pprint-str x))
     :cljs (pprint/pprint x)))

(defn pprint-str [x]
  #?(:clj (puget/pprint-str x)
     :cljs (with-out-str (pprint/pprint x))))

(defn int-pow [base exp]
  (int (Math/pow base exp)))

(defn str->int [s]
  (when (seq s)
    #?(:clj (Integer/parseInt s)
       :cljs (js/parseInt s))))

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
