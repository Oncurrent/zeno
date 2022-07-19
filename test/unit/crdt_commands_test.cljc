(ns unit.crdt-commands-test
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
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
      (kaocha.repl/run sym (merge {:color? false
                                   :capture-output? false} opts)))))

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

(defn ops->crdt [crdt-ops schema]
  (-> (apply-ops/apply-ops (u/sym-map crdt-ops schema))
      (:crdt)))

(defn ->value [crdt path schema]
  (crdt/get-value (u/sym-map crdt path schema)))

(comment (krun #'test-empty-map-of-records))
(deftest test-empty-map-of-records
  (is (= nil (->value {} [] pet-owners-schema)))
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
                            :zeno/path []}]
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
                            :zeno/path [:pet-owners]}]
                    :crdt crdt1)
        {crdt2 :crdt crdt2-ops :crdt-ops} (commands/process-cmds arg2)
        all-ops (set/union crdt1-ops crdt2-ops)
        acrdt (ops->crdt all-ops schema)]
    (is (= {} (->value crdt2 [:pet-owners] schema)))
    (is (= {} (->value acrdt [:pet-owners] schema)))
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
        _ (is (= {:norm-path [0] :value "Hello!" :exists? true}
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
              :crdt crdt1
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
        {crdt0 :crdt
         crdt0-ops :crdt-ops} (commands/process-cmds arg0)
        arg1 {:cmds [{:zeno/arg {:name "Chris"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-before
                      :zeno/path [:zeno/crdt :pets 0]}]
              :root :zeno/crdt
              :crdt crdt0
              :schema pet-owner-schema}
        arg2 {:cmds [{:zeno/arg {:name "Pat"
                                 :species "Canis familiaris"}
                      :zeno/op :zeno/insert-after
                      :zeno/path [:zeno/crdt :pets -1]}]
              :root :zeno/crdt
              :crdt crdt0
              :schema pet-owner-schema}
        ret1 (commands/process-cmds arg1)
        ret2 (commands/process-cmds arg2)
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2))
        {merged-crdt :crdt
         rc-ops :repair-crdt-ops} (apply-ops/apply-ops
                                   {:crdt crdt0
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
        merged-crdt (:crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                                 :crdt-ops new-crdt-ops
                                                 :schema pet-owner-schema}))
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
        new-crdt-ops (set/union (:crdt-ops ret1)
                                (:crdt-ops ret2)
                                (:crdt-ops ret3))
        merged-crdt (:crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                                 :crdt-ops new-crdt-ops
                                                 :schema schema}))
        ;; Order of the first three items is determined by their add-ids.
        ;; Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]).
        expected-value ["2" "3" "1" "A" "B"]
        v (crdt/get-value {:crdt merged-crdt
                           :path []
                           :schema schema})]
    (is (= expected-value v))))

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
        new-crdt-ops (set/union (:crdt-ops ret1)
                                (:crdt-ops ret2)
                                (:crdt-ops ret3))
        merged-crdt (:crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                                 :crdt-ops new-crdt-ops
                                                 :schema schema}))
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
        new-crdt-ops (set/union (:crdt-ops ret1)
                                (:crdt-ops ret2)
                                (:crdt-ops ret3))
        merged-crdt (:crdt (apply-ops/apply-ops {:crdt (:crdt ret0)
                                                 :crdt-ops new-crdt-ops
                                                 :schema schema}))
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
        all-ops (set/union (:crdt-ops ret1)
                           (:crdt-ops ret2))
        merged-crdt (:crdt (apply-ops/apply-ops
                            {:crdt (:crdt ret0)
                             :crdt-ops all-ops
                             :schema pet-owner-schema}))]
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
        value {"a" [1] "b" [2]}
        schema (l/map-schema (l/array-schema l/int-schema))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-set-nested-map-of-two-arrays))
(deftest test-set-nested-map-of-two-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value {"a" {"aa" [1]}
               "b" {"bb" [2]}}
        schema (l/map-schema (l/map-schema (l/array-schema l/int-schema)))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-arrays-set {:capture-output? true}))
(deftest test-crdt-nested-arrays-set
  (let [sys-time-ms (u/str->long "1643061294782")
        value [[[[[1 2]]]] [[[[3]] [[4]]]] [[[[5]]] [[[6 7]]]]]
        schema (-> l/int-schema l/array-schema l/array-schema
                   l/array-schema l/array-schema l/array-schema)
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-arrays-set-index))
(deftest test-crdt-nested-arrays-set-index
  (let [sys-time-ms (u/str->long "1643061294782")
        schema (l/array-schema (l/array-schema l/int-schema))
        arg {:cmds [{:zeno/arg [[1 2] [2]]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/arg [3]
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        expected-value [[1 2] [3]]
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

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
        schema (-> l/int-schema l/array-schema l/array-schema
                   l/map-schema l/map-schema)
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-set-remove-nested-arrays))
(deftest test-set-remove-nested-arrays
  (let [sys-time-ms (u/str->long "1643061294782")
        value [[1 2] [3]]
        schema (l/array-schema (l/array-schema l/int-schema))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 0 1]}
                    {:zeno/op :zeno/remove
                     :zeno/path [:zeno/crdt 1]}]
             :root :zeno/crdt
             :schema schema
             :sys-time-ms sys-time-ms}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [[1]]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array))
(deftest test-empty-array
  (let [value []
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array-as-value))
(deftest test-empty-array-as-value
  (let [value {"a" []}
        schema (l/map-schema (l/array-schema l/string-schema))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array-in-record))
(deftest test-empty-array-in-record
  (let [value []
        schema (l/record-schema
                :r1 [[:a (l/array-schema l/string-schema)]])
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt :a]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [:a] schema)
           (->value acrdt [:a] schema)))))

(comment (krun #'test-empty-array-in-map))
(deftest test-empty-array-in-map
  (let [value []
        schema (l/map-schema (l/array-schema l/string-schema))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/set
                     :zeno/path [:zeno/crdt "a"]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt ["a"] schema)
           (->value acrdt ["a"] schema)))))

(comment
 (krun #'test-record-nested-negative-insert-after))
(deftest test-record-nested-negative-insert-after
  (let [value "id"
        schema (l/record-schema :r1
                                [[:a (l/array-schema
                                      l/string-schema)]])
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt :a -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:a [value]}]
    (is (= value
           (->value crdt [:a -1] schema)
           (->value acrdt [:a -1] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-array-insert-after))
(deftest test-array-insert-after
  (let [value "id"
        schema (l/array-schema l/string-schema)
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [value]]
    (is (= value
           (->value crdt [-1] schema)
           (->value acrdt [-1] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-array-nested-insert-after))
(deftest test-array-nested-insert-after
  (let [value "id"
        schema (l/array-schema (l/array-schema l/string-schema))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt 0 -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [[value]]]
    (is (= value
           (->value crdt [0 -1] schema)
           (->value acrdt [0 -1] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-nested-negative-insert-after
  (let [value 42
        schema  (l/map-schema (l/map-schema (l/array-schema l/int-schema)))
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt "a" "b" 0]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"a" {"b" [value]}}]
    (is (= value
           (->value crdt ["a" "b" 0] schema)
           (->value acrdt ["a" "b" 0] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-deep-nested-negative-insert-after))
(deftest test-deep-nested-negative-insert-after
  (let [value "id"
        schema (l/record-schema
                :r1 [[:a (l/map-schema
                          (l/array-schema
                           (l/map-schema
                            (l/record-schema
                             :r2 [[:d (l/array-schema
                                       (l/array-schema
                                        l/string-schema))]]))))]])
        arg {:cmds [{:zeno/arg value
                     :zeno/op :zeno/insert-after
                     :zeno/path [:zeno/crdt :a "b" 0 "c" :d 0 -1]}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:a {"b" [{"c" {:d [[value]]}}]}}]
    (is (= value
           (->value crdt [:a "b" 0 "c" :d 0 -1] schema)
           (->value acrdt [:a "b" 0 "c" :d 0 -1] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

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
        schema tree-schema
        arg {:cmds [{:zeno/arg data
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= data
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(defn prefixed-uuid [prefix]
  (str prefix (u/compact-random-uuid)))

(defn prefixed-uuids [n prefix]
  (map #(str prefix %) (u/compact-random-uuids n)))

(defn gen-delete-cmd [i]
  {:zeno/op :zeno/remove
   :zeno/path [i]})

(defn gen-insert-before-cmd
  ([i]
   (gen-insert-before-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-before
    :zeno/path [i]
    :zeno/arg (prefixed-uuid prefix)}))

(defn gen-insert-after-cmd
  ([i]
   (gen-insert-after-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-after
    :zeno/path [i]
    :zeno/arg (prefixed-uuid prefix)}))

(defn gen-insert-range-before-cmd
  ([i]
   (gen-insert-range-before-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-range-before
    :zeno/path [i]
    :zeno/arg (prefixed-uuids 5 prefix)}))

(defn gen-insert-range-after-cmd
  ([i]
   (gen-insert-range-after-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-range-after
    :zeno/path [i]
    :zeno/arg (prefixed-uuids 5 prefix)}))

(defn rand-cmds-from-coll-of-strs
  ([coll]
   (rand-cmds-from-coll-of-strs coll ""))
  ([coll prefix]
   (keep-indexed (fn [i v]
                   (case (int (rand-int 6))
                     0 (gen-delete-cmd i)
                     2 (gen-insert-before-cmd i prefix)
                     1 (gen-insert-after-cmd i prefix)
                     4 (gen-insert-range-before-cmd i prefix)
                     3 (gen-insert-range-after-cmd i prefix)
                     5 nil))
                 coll)))

(defn get-deleted-values-as-set [data cmds]
  (->> cmds
       (filter (fn [{:zeno/keys [op]}]
                 (= :zeno/remove op)))
       (map (comp #(nth data %) first :zeno/path))
       (into #{})))

(defn filter-out-values [values-set data]
  (filter #(not (contains? values-set %)) data))

(defn keep-by-prefix [prefix data]
  (filter #(str/starts-with? % prefix) data))

(defn drop-by-prefix [prefix data]
  (filter #(not (str/starts-with? % prefix)) data))

(defn partition-by-prefix [prefix delimiter data]
  (partition-by #(= prefix (->> (re-pattern delimiter) (str/split %) (first)))
                data))

(defn partition-by-prefixes [delimeter data]
  (partition-by #(->> (re-pattern delimeter) (str/split %) (first)) data))

(defn drop-partitions-by-prefix [prefix data]
  (filter #(not (str/starts-with? (first %) prefix)) data))

(comment
 (kaocha.repl/run #'test-thing {:capture-output? false :color? false}))
(deftest test-thing []
  (let [;; First, we set up an initial CRDT and ensure it's correct.
        schema (l/array-schema l/string-schema)
        data (prefixed-uuids 3 "zeroth-")
        arg {:cmds [{:zeno/arg data
                     :zeno/op :zeno/set
                     :zeno/path []}]
             :root :zeno/crdt
             :schema schema}
        {:keys [crdt crdt-ops]} (commands/process-cmds arg)
        acrdt (ops->crdt crdt-ops schema)
        _ (is (= data
                 (->value crdt [] schema)
                 (->value acrdt [] schema)))
        ;; Next, we generate two sets of random commands concurrently (they
        ;; have no knowledge of each other).
        cmds1 (rand-cmds-from-coll-of-strs data "first-")
        cmds2 (rand-cmds-from-coll-of-strs data "second-")
        ;; Process each set of commands and extract the value from the resulting
        ;; CRDTs as well as construct CRDTs from the returned ops.
        {crdt1 :crdt crdt-ops1 :crdt-ops} (commands/process-cmds
                                           (assoc arg :cmds cmds1 :crdt crdt))
        {crdt2 :crdt crdt-ops2 :crdt-ops} (commands/process-cmds
                                           (assoc arg :cmds cmds2 :crdt crdt))
        acrdt1 (ops->crdt (set/union crdt-ops crdt-ops1) schema)
        acrdt2 (ops->crdt (set/union crdt-ops crdt-ops2) schema)
        data1 (->value crdt1 [] schema)
        data2 (->value crdt2 [] schema)
        ;; Some consistency checks for each CRDT individually.
        _ (is (= data1
                 (->value acrdt1 [] schema)))
        _ (is (= data2
                 (->value acrdt2 [] schema)))
        ;; Create the combined CRDT both ways, extract value, and assert
        ;; equality.
        acrdt-all (ops->crdt (set/union crdt-ops crdt-ops1 crdt-ops2) schema)
        mcrdt (-> (apply-ops/apply-ops {:crdt crdt
                                        :crdt-ops (set/union crdt-ops1 crdt-ops2)
                                        :schema schema})
                  (:crdt))
        data-all (->value acrdt-all [] schema)
        _ (is (= data-all (->value mcrdt [] schema)))
        ;; Because the values are UUIDs we can use sets instead of vectors for
        ;; the next two parts
        set1 (into #{} data1)
        set2 (into #{} data2)
        set-all (into #{} data-all)
        ;; Test that all not-deleted things are present post merge.
        _ (is (= set1 (set/intersection set-all set1)))
        _ (is (= set2 (set/intersection set-all set2)))
        ;; Test that deleted things are not present post merge.
        deleted1 (get-deleted-values-as-set data cmds1)
        deleted2 (get-deleted-values-as-set data cmds2)
        _ (is (empty? (set/intersection set-all deleted1)))
        _ (is (empty? (set/intersection set-all deleted2)))
        ;; Test that all zeroth things are in the correct order, cmds can't
        ;; rearrage without deleting and readding but we aren't doing that
        ;; since each new insertion is a new UUID.
        data-post-merge-expected (filter-out-values
                                  (set/union deleted1 deleted2) data)
        data-post-merge-actual (keep-by-prefix "zeroth" data-all)
        _ (is (= data-post-merge-expected data-post-merge-actual))
        ;; Test that at any given position, i.e. before or after an element of
        ;; the original array, there is not interleaving and that whichever
        ;; comes first at in one position comes first in all positions. We start
        ;; by creating partitions of all prefixes between all original elements.
        partitioned (->> data-all
                         (partition-by-prefix "zeroth" "-")
                         (drop-partitions-by-prefix "zeroth")
                         (map #(partition-by-prefixes "-" %)))
        *came-first (atom nil)
        *came-last (atom nil)
        _ (doseq [p partitioned]
            (case (count p)
              0 nil ; Not a problem, perhaps no commands were generated.
              1 nil ; Not a problem, testing would only be testing partition-by.
              2 (do
                 (when-not @*came-first
                   (reset! *came-first (-> p ffirst (str/split #"-") first)))
                 (when-not @*came-last
                   (reset! *came-last (-> p last first (str/split #"-") first)))
                 (is (and (every? #(str/starts-with? % @*came-first) (first p))
                         (every? #(str/starts-with? % @*came-last) (last p)))))
              (is (= "commands interleaved" "but should not have"))))]))
