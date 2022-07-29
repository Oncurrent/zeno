(ns unit.crdt-test
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as crdt-c]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns*))

(l/def-record-schema the-rec-schema
  [:foo/a l/int-schema]
  [:bar/b l/string-schema]
  [:c l/boolean-schema])

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(deftest test-single-value-crdt-basic-ops
  (let [data-schema l/string-schema
        root :crdt
        path [root]
        crdt-ops* [{:add-id "a1"
                    :op-type :add-value
                    :op-path path
                    :value "ABC"}
                   {:add-id "a1"
                    :op-type :delete-value
                    :op-path path}
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path path
                    :value "XYZ"}]
        expected-v "XYZ"]
    (doseq [crdt-ops (combo/permutations crdt-ops*)]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))]
        (is (= expected-v v))))))

(deftest test-union-crdt-basic-ops
  (let [data-schema (l/union-schema [l/null-schema l/string-schema l/int-schema])
        root :crdt
        path [root]
        ;; Note that the :sys-time-ms values are important here
        crdt-ops* [;; Union branch 1
                   {:add-id "a1"
                    :op-type :add-value
                    :op-path [root 1]
                    :sys-time-ms (u/str->long "1640205282850")
                    :value "ABC"}
                   {:add-id "a1"
                    :op-type :delete-value
                    :op-path [root 1]
                    :sys-time-ms (u/str->long "1640205282851")}
                   ;; Union branch 2
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path [root 2]
                    :sys-time-ms (u/str->long "1640205282852")
                    :value 42}]
        expected-v 42]
    (doseq [crdt-ops (combo/permutations crdt-ops*)]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))]
        (is (= expected-v v))))))

(deftest test-map-crdt-basic-ops
  (let [data-schema (l/map-schema l/int-schema)
        root :crdt
        crdt-ops* [{:add-id "a100"
                    :op-path [root]
                    :op-type :add-container}
                   {:add-id "a1"
                    :op-type :add-value
                    :op-path [root "a"]
                    :value 1}
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path [root "b"]
                    :value 2}
                   {:add-id "a1"
                    :op-type :delete-value
                    :op-path [root "a"]}
                   {:add-id "a3"
                    :op-type :add-value
                    :op-path [root "a"]
                    :value 42}
                   {:add-id "a2"
                    :op-type :delete-value
                    :op-path [root "b"]}]
        path [root]
        expected-v {"a" 42}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [crdt-ops (repeatedly 500 #(shuffle crdt-ops*))]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))]
        (is (= expected-v v))))))

(deftest test-nested-map-crdts
  (let [data-schema (l/map-schema (l/map-schema l/int-schema))
        root :crdt
        crdt-ops* [{:add-id "a100"
                    :op-path [root ]
                    :op-type :add-container}
                   {:add-id "a200"
                    :op-path [root "d"]
                    :op-type :add-container}
                   {:add-id "a0"
                    :op-type :add-value
                    :op-path [root "d" "w"]
                    :value 999}
                   {:add-id "a300"
                    :op-path [root "a"]
                    :op-type :add-container}
                   {:add-id "a1"
                    :op-type :add-value
                    :op-path [root "a" "x"]
                    :value 1}
                   {:add-id "az1"
                    :op-type :add-value
                    :op-path [root "a" "z"]
                    :value 84}
                   {:add-id "a400"
                    :op-path [root "b"]
                    :op-type :add-container}
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path [root "b" "y"]
                    :value 2}
                   {:add-id "a1"
                    :op-type :delete-value
                    :op-path [root "a" "x"]}
                   {:add-id "a3"
                    :op-type :add-value
                    :op-path [root "a" "x"]
                    :value 42}
                   {:add-id "a400"
                    :op-path [root "b"]
                    :op-type :delete-container}
                   {:add-id "a2"
                    :op-type :delete-value
                    :op-path [root "b" "y"]}]
        path [root]
        expected-v {"a" {"x" 42
                         "z" 84}
                    "d" {"w" 999}}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [crdt-ops (repeatedly 500 #(shuffle crdt-ops*))]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))]
        (is (= expected-v v))))))

(deftest test-record-crdt-basic-ops
  (let [data-schema the-rec-schema
        root :crdt
        crdt-ops* [{:add-id "a100"
                    :op-path [root]
                    :op-type :add-container}
                   {:add-id "a1"
                    :op-type :add-value
                    :op-path [root :foo/a 1]
                    :value 1}
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path [root :bar/b 1]
                    :value "Hi"}
                   {:add-id "a2"
                    :op-type :delete-value
                    :op-path [root :bar/b 1]}
                   {:add-id "a3"
                    :op-type :add-value
                    :op-path [root :bar/b 1]
                    :value "Yo"}
                   {:add-id "a4"
                    :op-type :add-value
                    :op-path [root :c 1]
                    :value true}
                   {:add-id "a4"
                    :op-type :delete-value
                    :op-path [root :c 1]}]
        path [root]
        expected-v {:foo/a 1
                    :bar/b "Yo"}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [crdt-ops (repeatedly 500 #(shuffle crdt-ops*))]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))]
        (is (= expected-v v))))))

(deftest test-record-crdt-conflict
  (let [data-schema the-rec-schema
        root :crdt
        crdt-1 (apply-ops/apply-ops
                {:crdt-ops [{:add-id "a100"
                             :op-path [root]
                             :op-type :add-container
                             :sys-time-ms (u/str->long "1640205282858")}
                            {:add-id "a1"
                             :op-type :add-value
                             :op-path [root :foo/a 1]
                             :sys-time-ms (u/str->long "1640205282858")
                             :value 1}]
                 :data-schema data-schema
                 :root root})
        crdt-2 (apply-ops/apply-ops
                {:crdt crdt-1
                 :crdt-ops [{:add-id "a200"
                             :op-path [root]
                             :op-type :add-container
                             :sys-time-ms (u/str->long "1640205282862")}
                            {:add-id "a2"
                             :op-type :add-value
                             :op-path [root :foo/a 1]
                             :sys-time-ms (u/str->long "1640205282862")
                             :value 42}]
                 :data-schema data-schema
                 :root root})
        v (get/get-value {:crdt crdt-2
                          :data-schema data-schema
                          :path [root]
                          :root root})
        expected-v {:foo/a 42}]
    (is (= expected-v v))))

(deftest test-nested-crdts
  (let [data-schema (l/map-schema the-rec-schema)
        root :crdt
        crdt-ops* [{:add-id "a100"
                    :op-path [root]
                    :op-type :add-container}
                   {:add-id "a200"
                    :op-path [root "a"]
                    :op-type :add-container}
                   {:add-id "a1"
                    :op-type :add-value
                    :op-path [root "a" :foo/a 1]
                    :value 1}
                   {:add-id "a1"
                    :op-type :delete-value
                    :op-path [root "a" :foo/a 1]}
                   {:add-id "a2"
                    :op-type :add-value
                    :op-path [root "a" :foo/a 1]
                    :value 222}
                   {:add-id "a300"
                    :op-path [root "b"]
                    :op-type :add-container}
                   {:add-id "a3"
                    :op-type :add-value
                    :op-path [root "b" :foo/a 1]
                    :value 72}
                   {:add-id "a4"
                    :op-type :add-value
                    :op-path [root "b" :bar/b 1]
                    :value "there"}
                   {:add-id "a5"
                    :op-type :add-value
                    :op-path [root "b" :c 1]
                    :value false}]
        path [root]
        expected-v {"a" {:foo/a 222}
                    "b" {:foo/a 72
                         :bar/b "there"
                         :c false}}]
    ;; Too many permutations to test them all, so we take a random sample
    (doseq [crdt-ops (repeatedly 500 #(shuffle crdt-ops*))]
      (let [crdt (apply-ops/apply-ops
                  (u/sym-map crdt-ops data-schema root))
            v (get/get-value (u/sym-map crdt data-schema path root))
            nested-v (get/get-value {:crdt crdt
                                     :data-schema data-schema
                                     :path [root "b" :bar/b]
                                     :root root})]
        (is (= expected-v v))
        (is (= "there" nested-v))))))

(deftest test-empty-stuff
  (let [data-schema l/string-schema
        root :crdt
        crdt-ops []
        path [root]
        crdt  (apply-ops/apply-ops
               (u/sym-map crdt-ops data-schema root))
        v (get/get-value (u/sym-map crdt data-schema path root))
        _ (is (= nil v))]))
