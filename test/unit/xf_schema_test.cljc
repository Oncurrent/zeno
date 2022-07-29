(ns unit.xf-schema-test
  (:require
   [deercreeklabs.lancaster :as l]
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.xf-schema :as xfs]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log]))

(comment (kaocha.repl/run *ns* {:capture-output? false}))

(def root :crdt)

(defn test-xf-schema [data-schema value]
  (let [crdt-schema (xfs/->crdt-schema (u/sym-map data-schema))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path [root]}]
        {crdt :crdt} (commands/process-cmds (u/sym-map cmds data-schema root))
        rt-crdt (->> crdt
                     (l/serialize crdt-schema)
                     (l/deserialize-same crdt-schema))]
    (is (= crdt rt-crdt))))

(deftest test-most-basic
  (test-xf-schema l/int-schema 1))

(deftest test-map-of-arrays-of-ints
  (test-xf-schema
   (l/map-schema (l/array-schema l/int-schema))
   {"a" [1 2 3] "b" [4 5 6] "c" [0 -1 -2]}))

(deftest test-nested-arrays-of-strings
  (test-xf-schema
   (l/array-schema (l/array-schema l/string-schema))
   [["a"] ["b" "c"] []]))

(deftest test-deep-nested
  (test-xf-schema
   (l/record-schema
    :r1 [[:a (l/map-schema
              (l/array-schema
               (l/map-schema
                (l/record-schema
                 :r2 [[:d (l/array-schema
                           (l/array-schema
                            l/string-schema))]]))))]])
   {:a {"b" [{"c" {:d [["e" "f"] []]}}]}}))

(l/def-record-schema tree-schema
  [:value l/int-schema]
  [:right-child ::tree]
  [:left-child ::tree])

;; TODO: Broken
(comment (kaocha.repl/run #'test-recursive {:capture-output? false :color? false}))
(deftest test-recursive
  (test-xf-schema
   tree-schema
   {:value 42
    :right-child {:value 3
                  :right-child {:value 8
                                :right-child {:value 21}
                                :left-child {:value 43}}
                  :left-child {:value 8
                               :right-child {:value 21}
                               :left-child {:value 43}}}
    :left-child {:value 3
                 :right-child {:value 8
                               :right-child {:value 21}
                               :left-child {:value 43}}
                 :left-child {:value 8
                              :right-child {:value 21}
                              :left-child {:value 43}}}}))
