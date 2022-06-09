(ns unit.crdt-commands-test
  (:require
   [clojure.set :as set]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops-impl :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.repair :as repair]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns* {:capture-output? false}))

#?(:clj
   (defn krun
     ([sym] (krun sym nil))
     ([sym opts]
      (kaocha.repl/run sym (merge {:color? false :capture-output? false} opts)))))

(l/def-record-schema pet-schema
  [:name l/string-schema]
  [:species l/string-schema])

(l/def-record-schema pet-owner-schema
  [:name l/string-schema]
  [:pets (l/array-schema pet-schema)])

(l/def-record-schema specialties-schema
  [:aggression l/boolean-schema]
  [:barking l/boolean-schema])

(l/def-record-schema pet-trainer-schema
  [:name l/string-schema]
  [:specialties specialties-schema])

(l/def-map-schema pet-owners-schema pet-owner-schema)

(l/def-record-schema pet-school-schema
  [:pet-owners (l/map-schema pet-owner-schema)])

(l/def-record-schema tree-schema
  [:value l/int-schema]
  [:right-child ::tree]
  [:left-child ::tree])

(defn ops->crdt [ops schema]
  (apply-ops/apply-ops {:crdt {} :crdt-ops ops :schema schema}))

(defn ->value [crdt path schema]
  (crdt/get-value (u/sym-map crdt path schema)))

(comment (krun #'test-empty-map-of-records))
(deftest test-empty-map-of-records
  (is (= {} (->value {} [] pet-owners-schema)))
  (is (= nil (->value {} ["a"] pet-owners-schema)))
  (is (= nil (->value {} [nil] pet-owners-schema))))

(comment (krun #'test-set-empty-map-of-records))
(deftest test-set-empty-map-of-records
  (let [schema pet-owners-schema
        arg {:cmds [{:zeno/arg {}
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= {}
           (->value crdt [] schema)
           (->value acrdt [] schema)))
    (is (= nil
           (->value crdt ["a"] schema)
           (->value acrdt ["a"] schema)))
    (is (= nil
           (->value crdt [nil] schema)
           (->value acrdt [nil] schema)))))

(comment (krun #'test-empty-record-with-map-of-records))
(deftest test-empty-record-with-map-of-records
  (is (= nil (->value {} [:pet-owners] pet-school-schema)))
  (is (= nil (->value {} [:pet-owners "a"] pet-school-schema)))
  (is (= nil (->value {} [:pet-owners nil] pet-school-schema))))

(comment (krun #'test-set-empty-record-with-map-of-records))
(deftest test-set-empty-record-with-map-of-records
  (let [schema pet-school-schema
        arg {:cmds [{:zeno/arg {}
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= nil
           (->value crdt [:pet-owners] schema)
           (->value acrdt [:pet-owners] schema)))
    (is (= nil
           (->value crdt [:pet-owners "a"] schema)
           (->value acrdt [:pet-owners "a"] schema)))
    (is (= nil
           (->value crdt [:pet-owners nil] schema)
           (->value acrdt [:pet-owners nil] schema)))))

(comment
 (krun #'test-set-then-reset-empty-record-with-empty-map-of-records))
(deftest test-set-then-reset-empty-record-with-empty-map-of-records
  (let [schema pet-school-schema
        arg1 {:cmds [{:zeno/arg {}
                      :zeno/op :zeno/set
                      :zeno/path []}]
              :schema schema}
        {crdt1 :crdt crdt1-ops :crdt-ops} (commands/process-cmds arg1)
        arg2 (assoc arg1
                    :cmds [{:zeno/arg {:pet-owners {}}
                            :zeno/op :zeno/set
                            :zeno-path []}]
                    :crdt crdt1)
        {crdt2 :crdt crdt2-ops :crdt-ops} (commands/process-cmds arg2)
        acrdt (ops->crdt (set/union crdt1-ops crdt2-ops) schema)]
    (is (= {}
           (->value crdt2 [:pet-owners] schema)
           (->value acrdt [:pet-owners] schema)))
    (is (= nil
           (->value crdt2 [:pet-owners "a"] schema)
           (->value acrdt [:pet-owners "a"] schema)))
    (is (= nil
           (->value crdt2 [:pet-owners nil] schema)
           (->value acrdt [:pet-owners nil] schema)))))

(comment
 (krun #'test-set-empty-record-then-path-set-with-empty-map-of-records))
(deftest test-set-empty-record-then-path-set-with-empty-map-of-records
  (let [schema pet-school-schema
        arg1 {:cmds [{:zeno/arg {}
                      :zeno/op :zeno/set
                      :zeno/path []}]
              :schema schema}
        {crdt1 :crdt crdt1-ops :crdt-ops} (commands/process-cmds arg1)
        arg2 (assoc arg1
                    :cmds [{:zeno/arg {}
                            :zeno/op :zeno/set
                            :zeno-path [:pet-owners]}]
                    :crdt crdt1)
        {crdt2 :crdt crdt2-ops :crdt-ops} (commands/process-cmds arg2)
        acrdt (ops->crdt (set/union crdt1-ops crdt2-ops) schema)]
    (is (= {}
           (->value crdt2 [:pet-owners] schema)
           (->value acrdt [:pet-owners] schema)))
    (is (= nil
           (->value crdt2 [:pet-owners "a"] schema)
           (->value acrdt [:pet-owners "a"] schema)))
    (is (= nil
           (->value crdt2 [:pet-owners nil] schema)
           (->value acrdt [:pet-owners nil] schema)))))

(comment
 (krun #'test-set-record-with-empty-map-of-records))
(deftest test-set-record-with-empty-map-of-records
  (let [schema pet-school-schema
        arg {:cmds [{:zeno/arg {:pet-owners {}}
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= {}
           (->value crdt [:pet-owners] schema)
           (->value acrdt [:pet-owners] schema)))
    (is (= nil
           (->value crdt [:pet-owners "a"] schema)
           (->value acrdt [:pet-owners "a"] schema)))
    (is (= nil
           (->value crdt [:pet-owners nil] schema)
           (->value acrdt [:pet-owners nil] schema)))))

(comment (krun #'test-crdt-set))
(deftest test-crdt-set
  (let [sys-time-ms (u/str->long "1643061294782")
        schema l/string-schema
        arg {:cmds [{:zeno/arg "Hi"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "Hi"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-set-and-remove))
(deftest test-crdt-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294782")
        schema l/string-schema
        arg {:cmds [{:zeno/arg "Hello"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value nil]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-set-and-reset))
(deftest test-crdt-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        schema l/string-schema
        arg {:cmds [{:zeno/arg "Hello"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Goodbye"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "Goodbye"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-map-set-and-reset))
(deftest test-crdt-map-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        schema (l/map-schema l/int-schema)
        arg {:cmds [{:zeno/arg 31
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Alice"]}
                    {:zeno/arg 8
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Bob"]}
                    {:zeno/arg 12
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "Bob"]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"Alice" 31 "Bob" 12}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-record-set-and-reset))
(deftest test-crdt-record-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        schema pet-schema
        arg {:cmds [{:zeno/arg "Lamby"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :name]}
                    {:zeno/arg "Ovis aries"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :species]}
                    {:zeno/arg "Sheepy"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :name]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Sheepy" :species "Ovis aries"}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-record-set-optional))
(deftest test-crdt-record-set-optional
  (let [sys-time-ms (u/str->long "1643061294782")
        schema pet-schema
        arg {:cmds [{:zeno/arg "Sheepy"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :name]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Sheepy"}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-record-set-and-remove))
(deftest test-crdt-nested-record-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294782")
        schema (l/array-schema pet-trainer-schema)
        arg {:cmds [{:zeno/arg [{:name "Bill"
                                 :specialties {:aggression true
                                               :barking false}}]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 0 :specialties :barking]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [{:name "Bill" :specialties {:aggression true}}]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-union-set-and-reset))
(deftest test-crdt-union-set-and-reset
  (let [sys-time-ms (u/str->long "1643061294782")
        schema (l/union-schema [l/string-schema l/float-schema])
        arg {:cmds [{:zeno/arg 3.14
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "pi"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "pi"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set))
(deftest test-crdt-array-set
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hi" "There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-empty))
(deftest test-crdt-array-set-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg []
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value []]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-pos))
(deftest test-crdt-array-set-index-pos
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "there"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hi" "Bob"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-neg))
(deftest test-crdt-array-set-index-neg
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "there"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt -2]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Bob" "there"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-out-of-bounds-pos))
(deftest test-crdt-array-set-index-out-of-bounds-pos
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["Hi" "there"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 2]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (commands/process-cmds arg)))))

(comment (krun #'test-crdt-array-set-index-out-of-bounds-neg))
(deftest test-crdt-array-set-index-out-of-bounds-neg
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg ["Hi" "there"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt -3]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (commands/process-cmds arg)))))

(comment (krun #'test-crdt-array-set-index-into-empty))
(deftest test-crdt-array-set-index-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        arg {:cmds [{:zeno/arg "Hi"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)
             :sys-time-ms sys-time-ms}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (commands/process-cmds arg)))))

(deftest test-crdt-array-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-maps-set-and-remove))
(deftest test-crdt-nested-maps-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/map-schema (l/map-schema l/int-schema))
        arg {:cmds [{:zeno/arg {"j" {"a" 1 "b" 2}
                                "k" {"y" 10 "z" 20}
                                "l" {"c" 3}}
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt "k" "y"]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"j" {"a" 1 "b" 2}
                        "k" {"z" 20}
                        "l" {"c" 3}}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-of-maps-set-and-remove))
(deftest test-crdt-array-of-maps-set-and-remove
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema (l/map-schema l/int-schema))
        arg {:cmds [{:zeno/arg [{"a" 1 "b" 2}
                                {"y" 10 "z" 20}
                                {"c" 3}]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 1 "y"]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [{"a" 1 "b" 2}
                        {"z" 20}
                        {"c" 3}]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-set-and-reset
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Go"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Go" "There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-simple-inserts
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["Hi" "There"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-before
                     :zeno/path [:zeno/crdt 0]}
                    {:zeno/arg "Bob"
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hello!" "Hi" "There" "Bob"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-before-into-empty
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-before
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hello!"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-after-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg "Hello!"
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        _ (is (= {:norm-path [0] :value "Hello!"}
                 (crdt/get-value-info {:crdt crdt :path [-1] :schema schema})
                 (crdt/get-value-info {:crdt acrdt :path [-1] :schema schema})))
        expected-value ["Hello!"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-insert-range-after-into-empty))
(deftest test-crdt-array-insert-range-after-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-before-into-empty
  (let [sys-time-ms (u/str->long "1643061294999")
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-insert-range-before-into-front))
(deftest test-crdt-array-insert-range-before-into-front
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["4" "5"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt 0]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3" "4" "5"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-after-end
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["4" "5"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["4" "5" "1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-before-into-middle
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["A" "B"]
                     :zeno/op :zeno/insert-range-before
                     :zeno/path [:zeno/crdt -2]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "A" "B" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-after-into-middle
  (let [schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg ["1" "2" "3"]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg ["A" "B"]
                     :zeno/op :zeno/insert-range-after
                     :zeno/path [:zeno/crdt -2]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "A" "B" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-nested-set
  (let [schema pet-owner-schema
        arg {:cmds [{:zeno/arg {:name "Bill"
                                :pets [{:name "Pinky"
                                        :species "Felis catus"}
                                       {:name "Fishy"
                                        :species "Carassius auratus"}]}
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg "Goldy"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :pets -1 :name]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Bill"
                        :pets [{:name "Pinky"
                                :species "Felis catus"}
                               {:name "Goldy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))
    (is (= {:name "Pinky"
            :species "Felis catus"}
           (->value crdt [:pets -2] schema)
           (->value acrdt [:pets -2] schema)))))

(deftest test-nested-set-and-remove
  (let [*next-id-num (atom 0)
        schema pet-owner-schema
        arg {:cmds [{:zeno/arg {:name "Bill"
                                :pets [{:name "Pinky"
                                        :species "Felis catus"}
                                       {:name "Fishy"
                                        :species "Carassius auratus"}]}
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt :pets 0]}]
             :root :zeno/crdt
             :schema schema
             :make-id #(let [n (swap! *next-id-num inc)]
                         (str "I" n))}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-subsequent-txns
  (let [schema pet-owner-schema
        arg1 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema schema}
        {crdt1 :crdt crdt1-ops :crdt-ops} (commands/process-cmds arg1)
        arg2 {:cmds [{:zeno/op :zeno/remove
                      :zeno/path [:zeno/crdt :pets 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret1)
              :schema schema}
        {crdt2 :crdt crdt2-ops :crdt-ops} (commands/process-cmds arg2)
        acrdt (ops->crdt (set/union crdt1-ops crdt2-ops) schema)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value
           (->value crdt2 [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-nested-merge-array-no-conflict))
(deftest test-nested-merge-array-no-conflict
  (let [arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg {:name "Chris"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema}
        arg2 {:cmds [{:zeno/arg {:name "Pat"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt :pets -1]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2))
        merged-crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                          :crdt-ops new-crdt-ops
                                          :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}
                        {:name "Pat"
                         :species "Canis familiaris"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-nested-merge-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema pet-owner-schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg {:name "Chris"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg {:name "Pat"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2))
        merged-crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                          :crdt-ops new-crdt-ops
                                          :schema pet-owner-schema})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pat"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path [:pets]
                                           :schema pet-owner-schema})))))

(deftest test-merge-3-way-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "2"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "3"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2) (:crdt-ops ret3))
        merged-crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                          :crdt-ops new-crdt-ops
                                          :schema schema})
        ;; Order of the first three items is determined by their add-ids.
        ;; Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]).
        expected-value ["2" "3" "1" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-3-way-array-conflict-multiple-peer-cmds
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "X1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "X2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "Y1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "Y2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "Z1"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "Z2"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt 0]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2) (:crdt-ops ret3))
        merged-crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                          :crdt-ops new-crdt-ops
                                          :schema schema})
        ;; Order of the first three pairs of items is determined by their
        ;; add-ids. Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]) and that the Xs, Ys, and Zs are not interleaved.
        expected-value ["Y1" "Y2" "Z1" "Z2" "X1" "X2" "A" "B"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :path []
                                           :schema schema})))))

(deftest test-merge-multiple-conflicts
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        arg0 {:cmds [{:zeno/arg ["A" "B" "C"]
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema schema
              :make-id make-id}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "XF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "XL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg2 {:cmds [{:zeno/arg "YF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "YL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        arg3 {:cmds [{:zeno/arg "ZF"
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt 0]}
                     {:zeno/arg "ZL"
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt -1]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema schema
              :make-id make-id}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        ret3 (commands/process-cmds arg3)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2) (:crdt-ops ret3))
        merged-crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                          :crdt-ops new-crdt-ops
                                          :schema schema})
        expected-value ["XF" "YF" "ZF" "A" "B" "C" "XL" "YL" "ZL"]]
    (is (= expected-value (crdt/get-value {:crdt merged-crdt
                                           :make-id make-id
                                           :path []
                                           :schema schema})))))

(deftest test-nested-merge-conflict
  (let [arg0 {:cmds [{:zeno/arg {:name "Bill"
                                 :pets [{:name "Pinky"
                                         :species "Felis catus"}
                                        {:name "Fishy"
                                         :species "Carassius auratus"}]}
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt]}]
              :root :zeno/crdt
              :schema pet-owner-schema}
        ret0 (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg "Goldy"
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt :pets -1 :name]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282000")}
        arg2 {:cmds [{:zeno/arg "Herman"
                      :zeno/op :zeno/set
                      :zeno/path [:zeno/crdt :pets -1 :name]}]
              :root :zeno/crdt
              :crdt (:crdt ret0)
              :schema pet-owner-schema
              :sys-time-ms (u/str->long "1640205282999")}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        merged-crdt (apply-ops/apply-ops
                     {:crdt (:crdt ret0)
                      :crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2))
                      :schema pet-owner-schema})]
    ;; We expect "Herman" because its sys-time-ms is later
    (is (= "Herman" (crdt/get-value {:crdt merged-crdt
                                     :path [:pets -1 :name]
                                     :schema pet-owner-schema})))))

(deftest test-set-into-empty-array
  (let [arg {:cmds [{:zeno/arg "1"
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"arg is not sequential"
         (commands/process-cmds arg)))))

(comment (krun #'test-set-map-of-two-arrays))
(deftest test-set-map-of-two-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value {"a" [1]
               "b" [2]}
        path [:zeno/crdt]
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path path}]
             :root :zeno/crdt
             :schema (l/map-schema (l/array-schema l/int-schema))
             :sys-time-ms sys-time-ms}
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-set-nested-map-of-two-arrays))
(deftest test-set-nested-map-of-two-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value {"a" {"aa" [1]}
               "b" {"bb" [2]}}
        path [:zeno/crdt]
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path path}]
             :root :zeno/crdt
             :schema (l/map-schema (l/map-schema (l/array-schema l/int-schema)))
             :sys-time-ms sys-time-ms}
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-crdt-nested-arrays-set {:capture-output? true}))
(deftest test-crdt-nested-arrays-set
  (let [sys-time-ms (u/str->long "1643061294782")
        value [[[[[1 2]]]] [[[[3]] [[4]]]] [[[[5]]] [[[6 7]]]]]
        path [:zeno/crdt]
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path path}]
             :root :zeno/crdt
             :schema (-> l/int-schema l/array-schema l/array-schema
                         l/array-schema l/array-schema l/array-schema)
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-crdt-nested-arrays-set-index))
(deftest test-crdt-nested-arrays-set-index
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg [[1 2] [2]]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg [3]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema (l/array-schema (l/array-schema l/int-schema))
             :sys-time-ms sys-time-ms}
        expected-value [[1 2] [3]]
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(comment (krun #'test-crdt-nested-arrays-set-index-out-of-bounds))
(deftest test-crdt-nested-arrays-set-index-out-of-bounds
  (let [sys-time-ms (u/str->long "1643061294782")
        arg {:cmds [{:zeno/arg [[1 2]]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg [3]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema (l/array-schema (l/array-schema l/int-schema))
             :sys-time-ms sys-time-ms}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (commands/process-cmds arg)))))

(comment (krun #'test-set-nested-map-of-two-nested-arrays))
(deftest test-set-nested-map-of-two-nested-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value {"a" {"aa" [[1 2] [3]]}
               "b" {"bb" [[4 5] [6]]}}
        path [:zeno/crdt]
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path path}]
             :root :zeno/crdt
             :schema (l/map-schema
                      (l/map-schema
                       (l/array-schema
                        (l/array-schema l/int-schema))))
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-set-remove-nested-arrays))
(deftest test-set-remove-nested-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value [[1 2] [3]]
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 0 1]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema (l/array-schema (l/array-schema l/int-schema))
             :sys-time-ms sys-time-ms}
        expected-value [[1]]
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(comment (krun #'test-empty-array))
(deftest test-empty-array
  (let [value []
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)}
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-empty-array-as-value))
(deftest test-empty-array-as-value
  (let [value {"a" []}
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema (l/map-schema (l/array-schema l/string-schema))}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path []
                                  :schema (:schema arg)})))))

(comment (krun #'test-empty-array-in-record))
(deftest test-empty-array-in-record
  (let [value []
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :a]}]
             :root :zeno/crdt
             :schema (l/record-schema
                      :r1 [[:a (l/array-schema l/string-schema)]])}
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path [:a]
                                  :schema (:schema arg)})))))

(comment (krun #'test-empty-array-in-map))
(deftest test-empty-array-in-map
  (let [value []
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "a"]}]
             :root :zeno/crdt
             :schema (l/map-schema (l/array-schema l/string-schema))}
        {:keys [crdt]} (commands/process-cmds arg)]
    (is (= value (crdt/get-value {:crdt crdt
                                  :path ["a"]
                                  :schema (:schema arg)})))))
(comment
 (krun #'test-record-nested-negative-insert-after))
(deftest test-record-nested-negative-insert-after
  (let [value "id"
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt :a -1]}]
             :root :zeno/crdt
             :schema (l/record-schema :r1
                                      [[:a (l/array-schema
                                            l/string-schema)]])}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        expected-value {:a [value]}
        applied-crdt (apply-ops/apply-ops {:crdt {}
                                           :crdt-ops crdt-ops
                                           :schema (:schema arg)})
        repaired-crdt (:crdt (repair/repair
                              (assoc arg :crdt applied-crdt)))]
    (is (= crdt repaired-crdt))
    (is (= value (crdt/get-value {:crdt crdt
                                  :path [:a -1]
                                  :schema (:schema arg)})))
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(comment (krun #'test-array-insert-after))
(deftest test-array-insert-after
  (let [value "id"
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema (l/array-schema l/string-schema)}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        expected-value [value]
        applied-crdt (apply-ops/apply-ops {:crdt {}
                                           :crdt-ops crdt-ops
                                           :schema (:schema arg)})
        repaired-crdt (:crdt (repair/repair
                              (assoc arg :crdt applied-crdt)))]
    (is (= crdt repaired-crdt))
    (is (= value (crdt/get-value {:crdt crdt
                                  :path [-1]
                                  :schema (:schema arg)})))
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(comment (krun #'test-array-nested-insert-after))
(deftest ^:this test-array-nested-insert-after
  (let [value "id"
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt 0 -1]}]
             :root :zeno/crdt
             :schema (l/array-schema (l/array-schema l/string-schema))}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        expected-value [[value]]
        applied-crdt (apply-ops/apply-ops {:crdt {}
                                           :crdt-ops crdt-ops
                                           :schema (:schema arg)})
        repaired-crdt (:crdt (repair/repair
                              (assoc arg :crdt applied-crdt)))]
    (is (= crdt repaired-crdt))
    (is (= value (crdt/get-value {:crdt crdt
                                  :path [0 -1]
                                  :schema (:schema arg)})))
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(comment (krun #'test-deep-nested-negative-insert-after))
(deftest test-deep-nested-negative-insert-after
  (let [value "id"
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt :a "b" 0 "c" :d 0 -1]}]
             :root :zeno/crdt
             :schema (l/record-schema
                      :r1 [[:a (l/map-schema
                                (l/array-schema
                                 (l/map-schema
                                  (l/record-schema
                                   :r2 [[:d (l/array-schema
                                             (l/array-schema
                                              l/string-schema))]]))))]])}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        expected-value {:a {"b" [{"c" {:d [[value]]}}]}}
        applied-crdt (apply-ops/apply-ops {:crdt {}
                                           :crdt-ops crdt-ops
                                           :schema (:schema arg)})
        repaired-crdt (:crdt (repair/repair
                              (assoc arg :crdt applied-crdt)))]
    (is (= crdt repaired-crdt))
    (is (= value (crdt/get-value {:crdt crdt
                                  :path [:a "b" 0 "c" :d 0 -1]
                                  :schema (:schema arg)})))
    (is (= expected-value (crdt/get-value {:crdt crdt
                                           :path []
                                           :schema (:schema arg)})))))

(deftest test-recursive-schema
  (let [data {:value 42
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
                                        :left-child {:value 43}}}}
        arg {:cmds [{:zeno/arg data
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :root :zeno/crdt
             :schema tree-schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        rt-data (crdt/get-value {:crdt crdt
                                 :path []
                                 :schema tree-schema})]
    (is (= data rt-data))))
