(ns unit.crdt-commands-test
  (:require
   [clojure.data :as data]
   [clojure.set :as set]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [com.oncurrent.zeno.state-providers.crdt.apply-ops :as apply-ops]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
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

(def root :crdt)

(defn process-cmds
  ([cmds data-schema]
   (process-cmds cmds data-schema nil))
  ([cmds data-schema {:keys [crdt make-id]}]
   (let [cmds (map (fn [cmd]
                     (update cmd :zeno/path #(vec (cons root %))))
                   cmds)]
     (commands/process-cmds (u/sym-map cmds data-schema root crdt make-id)))))

(defn ops->crdt
  ([crdt-ops data-schema]
   (ops->crdt crdt-ops data-schema nil))
  ([crdt-ops data-schema {:keys [crdt]}]
   (apply-ops/apply-ops (u/sym-map crdt-ops data-schema root crdt))))

(defn ->value [crdt path data-schema]
  (let [path (concat [root] path)]
    (get/get-in-state (u/sym-map crdt path data-schema root))))

(comment (krun #'test-empty-map-of-records))
(deftest test-empty-map-of-records
  (is (= nil (->value {} [] pet-owners-schema)))
  (is (= nil (->value {} ["a"] pet-owners-schema)))
  (is (= nil (->value {} [nil] pet-owners-schema))))

(comment (krun #'test-set-empty-map-of-records))
(deftest test-set-empty-map-of-records
  (let [cmds [{:zeno/arg {}
               :zeno/op :zeno/set
               :zeno/path []}]
        schema pet-owners-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
  (let [cmds [{:zeno/arg {}
               :zeno/op :zeno/set
               :zeno/path []}]
        schema pet-school-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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

(comment (krun #'test-set-then-reset-empty-record-with-empty-map-of-records))
(deftest test-set-then-reset-empty-record-with-empty-map-of-records
  (let [schema pet-school-schema
        cmds1 [{:zeno/arg {}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt1 :crdt crdt1-ops :crdt-ops} (process-cmds cmds1 schema)
        cmds2 [{:zeno/arg {:pet-owners {}}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt2 :crdt crdt2-ops :crdt-ops} (process-cmds cmds2 schema {:crdt crdt1})
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

(comment (krun #'test-set-empty-record-then-path-set-with-empty-map-of-records))
(deftest test-set-empty-record-then-path-set-with-empty-map-of-records
  (let [schema pet-school-schema
        cmds1 [{:zeno/arg {}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt1 :crdt crdt1-ops :crdt-ops} (process-cmds cmds1 schema)
        cmds2 [{:zeno/arg {}
                :zeno/op :zeno/set
                :zeno/path [:pet-owners]}]
        {crdt2 :crdt crdt2-ops :crdt-ops} (process-cmds cmds2 schema {:crdt crdt1})
        all-ops (set/union crdt1-ops crdt2-ops)
        acrdt (ops->crdt all-ops schema)]
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
  (let [cmds [{:zeno/arg {:pet-owners {}}
               :zeno/op :zeno/set
               :zeno/path []}]
        schema pet-school-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
  (let [cmds [{:zeno/arg "Hi"
               :zeno/op :zeno/set
               :zeno/path []}]
        schema l/string-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "Hi"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-set-and-remove))
(deftest test-crdt-set-and-remove
  (let [cmds [{:zeno/arg "Hello"
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path []}]
        schema l/string-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value nil]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-set-and-reset))
(deftest test-crdt-set-and-reset
  (let [cmds [{:zeno/arg "Hello"
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Goodbye"
               :zeno/op :zeno/set
               :zeno/path []}]
        schema l/string-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "Goodbye"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-map-set-and-reset))
(deftest test-crdt-map-set-and-reset
  (let [cmds [{:zeno/arg 31
               :zeno/op :zeno/set
               :zeno/path ["Alice"]}
              {:zeno/arg 8
               :zeno/op :zeno/set
               :zeno/path ["Bob"]}
              {:zeno/arg 12
               :zeno/op :zeno/set
               :zeno/path ["Bob"]}]
        schema (l/map-schema l/int-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"Alice" 31 "Bob" 12}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

;; TODO: Broken
(comment (krun #'test-crdt-record-set-and-reset))
(deftest test-crdt-record-set-and-reset
  (let [cmds [{:zeno/arg "Lamby"
               :zeno/op :zeno/set
               :zeno/path [:name]}
              {:zeno/arg "Ovis aries"
               :zeno/op :zeno/set
               :zeno/path [:species]}
              {:zeno/arg "Sheepy"
               :zeno/op :zeno/set
               :zeno/path [:name]}]
        schema pet-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Sheepy" :species "Ovis aries"}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-record-set-optional))
(deftest test-crdt-record-set-optional
  (let [cmds [{:zeno/arg "Sheepy"
               :zeno/op :zeno/set
               :zeno/path [:name]}]
        schema pet-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Sheepy"}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-record-set-and-remove))
(deftest test-crdt-nested-record-set-and-remove
  (let [cmds [{:zeno/arg [{:name "Bill"
                           :specialties {:aggression true
                                         :barking false}}]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path [0 :specialties :barking]}]
        schema (l/array-schema pet-trainer-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [{:name "Bill" :specialties {:aggression true}}]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-union-set-and-reset))
(deftest test-crdt-union-set-and-reset
  (let [cmds [{:zeno/arg 3.14
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "pi"
               :zeno/op :zeno/set
               :zeno/path []}]
        schema (l/union-schema [l/string-schema l/float-schema])
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value "pi"]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set))
(deftest test-crdt-array-set
  (let [cmds [{:zeno/arg ["Hi" "There"]
               :zeno/op :zeno/set
               :zeno/path []}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hi" "There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-empty))
(deftest test-crdt-array-set-empty
  (let [cmds [{:zeno/arg []
               :zeno/op :zeno/set
               :zeno/path []}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value []]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-pos))
(deftest test-crdt-array-set-index-pos
  (let [cmds [{:zeno/arg ["Hi" "there"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Bob"
               :zeno/op :zeno/set
               :zeno/path [1]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hi" "Bob"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-neg))
(deftest test-crdt-array-set-index-neg
  (let [cmds [{:zeno/arg ["Hi" "there"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Bob"
               :zeno/op :zeno/set
               :zeno/path [-2]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Bob" "there"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-set-index-out-of-bounds-pos))
(deftest test-crdt-array-set-index-out-of-bounds-pos
  (let [cmds [{:zeno/arg ["Hi" "there"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Bob"
               :zeno/op :zeno/set
               :zeno/path [2]}]]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (process-cmds cmds (l/array-schema l/string-schema))))))

(comment (krun #'test-crdt-array-set-index-out-of-bounds-neg))
(deftest test-crdt-array-set-index-out-of-bounds-neg
  (let [cmds [{:zeno/arg ["Hi" "there"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Bob"
               :zeno/op :zeno/set
               :zeno/path [-3]}]]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (process-cmds cmds (l/array-schema l/string-schema))))))

(comment (krun #'test-crdt-array-set-index-into-empty))
(deftest test-crdt-array-set-index-into-empty
  (let [cmds [{:zeno/arg "Hi"
               :zeno/op :zeno/set
               :zeno/path [0]}]]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (process-cmds cmds (l/array-schema l/string-schema))))))

(deftest test-crdt-array-set-and-remove
  (let [cmds [{:zeno/arg ["Hi" "There"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path [0]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-maps-set-and-remove))
(deftest test-crdt-nested-maps-set-and-remove
  (let [cmds [{:zeno/arg {"j" {"a" 1 "b" 2}
                          "k" {"y" 10 "z" 20}
                          "l" {"c" 3}}
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path ["k" "y"]}]
        schema (l/map-schema (l/map-schema l/int-schema))
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"j" {"a" 1 "b" 2}
                        "k" {"z" 20}
                        "l" {"c" 3}}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-of-maps-set-and-remove))
(deftest test-crdt-array-of-maps-set-and-remove
  (let [cmds [{:zeno/arg [{"a" 1 "b" 2}
                          {"y" 10 "z" 20}
                          {"c" 3}]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path [1 "y"]}]
        schema (l/array-schema (l/map-schema l/int-schema))
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [{"a" 1 "b" 2}
                        {"z" 20}
                        {"c" 3}]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-set-and-reset
  (let [cmds [{:zeno/arg ["Hi" "There"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Go"
               :zeno/op :zeno/set
               :zeno/path [0]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Go" "There"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-simple-inserts
  (let [cmds [{:zeno/arg ["Hi" "There"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Hello!"
               :zeno/op :zeno/insert-before
               :zeno/path [0]}
              {:zeno/arg "Bob"
               :zeno/op :zeno/insert-after
               :zeno/path [-1]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hello!" "Hi" "There" "Bob"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-before-into-empty
  (let [cmds [{:zeno/arg "Hello!"
               :zeno/op :zeno/insert-before
               :zeno/path [0]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["Hello!"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-after-into-empty
  (let [cmds [{:zeno/arg "Hello!"
               :zeno/op :zeno/insert-after
               :zeno/path [-1]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        _ (is (= "Hello!"
               (->value crdt [-1] schema)
               (->value acrdt [-1] schema)))
        expected-value ["Hello!"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-insert-range-after-into-empty))
(deftest test-crdt-array-insert-range-after-into-empty
  (let [cmds [{:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/insert-range-after
               :zeno/path [-1]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-before-into-empty
  (let [cmds [{:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/insert-range-before
               :zeno/path [0]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-array-insert-range-before-into-front))
(deftest test-crdt-array-insert-range-before-into-front
  (let [cmds [{:zeno/arg ["4" "5"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/insert-range-before
               :zeno/path [0]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "3" "4" "5"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-after-end
  (let [cmds [{:zeno/arg ["4" "5"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/insert-range-after
               :zeno/path [-1]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["4" "5" "1" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-before-into-middle
  (let [cmds [{:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg ["A" "B"]
               :zeno/op :zeno/insert-range-before
               :zeno/path [-2]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "A" "B" "2" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-crdt-array-insert-range-after-into-middle
  (let [cmds [{:zeno/arg ["1" "2" "3"]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg ["A" "B"]
               :zeno/op :zeno/insert-range-after
               :zeno/path [-2]}]
        schema (l/array-schema l/string-schema)
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value ["1" "2" "A" "B" "3"]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-nested-set
  (let [cmds [{:zeno/arg {:name "Bill"
                          :pets [{:name "Pinky"
                                  :species "Felis catus"}
                                 {:name "Fishy"
                                  :species "Carassius auratus"}]}
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg "Goldy"
               :zeno/op :zeno/set
               :zeno/path [:pets -1 :name]}]
        schema pet-owner-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
  (let [cmds [{:zeno/arg {:name "Bill"
                          :pets [{:name "Pinky"
                                  :species "Felis catus"}
                                 {:name "Fishy"
                                  :species "Carassius auratus"}]}
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path [:pets 0]}]
        *next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema pet-owner-schema
        {:keys [crdt crdt-ops]} (process-cmds cmds schema {:make-id make-id})
        acrdt (ops->crdt crdt-ops schema)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-subsequent-txns
  (let [schema pet-owner-schema
        cmds1 [{:zeno/arg {:name "Bill"
                           :pets [{:name "Pinky"
                                   :species "Felis catus"}
                                  {:name "Fishy"
                                   :species "Carassius auratus"}]}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt1 :crdt crdt1-ops :crdt-ops} (process-cmds cmds1 schema)
        cmds2 [{:zeno/op :zeno/remove
                :zeno/path [:pets 0]}]
        {crdt2 :crdt crdt2-ops :crdt-ops} (process-cmds
                                           cmds2 schema {:crdt crdt1})
        acrdt (ops->crdt (set/union crdt1-ops crdt2-ops) schema)
        expected-value {:name "Bill"
                        :pets [{:name "Fishy"
                                :species "Carassius auratus"}]}]
    (is (= expected-value
           (->value crdt2 [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-nested-merge-array-no-conflict))
(deftest test-nested-merge-array-no-conflict
  (let [schema pet-owner-schema
        cmds0 [{:zeno/arg {:name "Bill"
                           :pets [{:name "Pinky"
                                   :species "Felis catus"}
                                  {:name "Fishy"
                                   :species "Carassius auratus"}]}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt0 :crdt crdt0-ops :crdt-ops} (process-cmds cmds0 schema)
        cmds1 [{:zeno/arg {:name "Chris"
                           :species "Canis familiaris"}
                :zeno/op :zeno/insert-before
                :zeno/path [:pets 0]}]
        cmds2 [{:zeno/arg {:name "Pat"
                           :species "Canis familiaris"}
                :zeno/op :zeno/insert-after
                :zeno/path [:pets -1]}]
        ret1 (process-cmds cmds1 schema {:crdt crdt0})
        ret2 (process-cmds cmds2 schema {:crdt crdt0})
        new-crdt-ops (set/union (:crdt-ops ret1) (:crdt-ops ret2))
        merged-crdt (ops->crdt new-crdt-ops schema {:crdt crdt0})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}
                        {:name "Pat"
                         :species "Canis familiaris"}]]
    (is (= expected-value (->value merged-crdt [:pets] schema)))))

(deftest test-nested-merge-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema pet-owner-schema
        cmds0 [{:zeno/arg {:name "Bill"
                           :pets [{:name "Pinky"
                                   :species "Felis catus"}
                                  {:name "Fishy"
                                   :species "Carassius auratus"}]}
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt0 :crdt
         crdt-ops :crdt-ops} (process-cmds cmds0 schema {:make-id make-id})
        cmds1 [{:zeno/arg {:name "Chris"
                           :species "Canis familiaris"}
                :zeno/op :zeno/insert-before
                :zeno/path [:pets 0]}]
        cmds2 [{:zeno/arg {:name "Pat"
                           :species "Canis familiaris"}
                :zeno/op :zeno/insert-before
                :zeno/path [:pets 0]}]
        {crdt1 :crdt
         crdt1-ops :crdt-ops} (process-cmds cmds1 schema
                                            {:crdt crdt0 :make-id make-id})
        {crdt2 :crdt
         crdt2-ops :crdt-ops} (process-cmds cmds2 schema
                                            {:crdt crdt0 :make-id make-id})
        new-crdt-ops (set/union crdt1-ops crdt2-ops)
        merged-crdt (ops->crdt new-crdt-ops schema {:crdt crdt0})
        expected-value [{:name "Chris"
                         :species "Canis familiaris"}
                        {:name "Pat"
                         :species "Canis familiaris"}
                        {:name "Pinky"
                         :species "Felis catus"}
                        {:name "Fishy"
                         :species "Carassius auratus"}]]
    (is (= expected-value (->value merged-crdt [:pets] schema)))))

(deftest test-merge-3-way-array-conflict
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        cmds0 [{:zeno/arg ["A" "B"]
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt0 :crdt
         crdt0-ops :crdt-ops} (process-cmds cmds0 schema {:make-id make-id})
        cmds1 [{:zeno/arg "1"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}]
        cmds2 [{:zeno/arg "2"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}]
        cmds3 [{:zeno/arg "3"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}]
        {crdt1 :crdt
         crdt1-ops :crdt-ops} (process-cmds
                               cmds1 schema {:crdt crdt0 :make-id make-id})
        {crdt2 :crdt
         crdt2-ops :crdt-ops} (process-cmds
                               cmds2 schema {:crdt crdt0 :make-id make-id})
        {crdt3 :crdt
         crdt3-ops :crdt-ops} (process-cmds
                               cmds3 schema {:crdt crdt0 :make-id make-id})
        new-crdt-ops (set/union crdt1-ops crdt2-ops crdt3-ops)
        merged-crdt (ops->crdt new-crdt-ops schema {:crdt crdt0})
        ;; Order of the first three items is determined by their add-ids.
        ;; Any ordering is fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]).
        expected-value ["3" "1" "2" "A" "B"]]
    (is (= expected-value (->value merged-crdt [] schema)))))

(deftest test-merge-3-way-array-conflict-multiple-peer-cmds
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        cmds0 [{:zeno/arg ["A" "B"]
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt0 :crdt
         crdt0-ops :crdt-ops} (process-cmds cmds0 schema {:make-id make-id})
        cmds1 [{:zeno/arg "X1"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "X2"
                :zeno/op :zeno/insert-after
                :zeno/path [0]}]
        cmds2 [{:zeno/arg "Y1"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "Y2"
                :zeno/op :zeno/insert-after
                :zeno/path [0]}]
        cmds3 [{:zeno/arg "Z1"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "Z2"
                :zeno/op :zeno/insert-after
                :zeno/path [0]}]
        {crdt1 :crdt
         crdt1-ops :crdt-ops} (process-cmds
                               cmds1 schema {:crdt crdt0 :make-id make-id})
        {crdt2 :crdt
         crdt2-ops :crdt-ops} (process-cmds
                               cmds2 schema {:crdt crdt0 :make-id make-id})
        {crdt3 :crdt
         crdt3-ops :crdt-ops} (process-cmds
                               cmds3 schema {:crdt crdt0 :make-id make-id})
        new-crdt-ops (set/union crdt1-ops crdt2-ops crdt3-ops)
        merged-crdt (ops->crdt new-crdt-ops schema {:crdt crdt0})
        ;; Order of the first three pairs of items is determined by their
        ;; add-ids. Any ordering would be fine, as long as it is deterministic.
        ;; The important part is that all three appear before the original
        ;; sequence (["A" "B"]) and that the Xs, Ys, and Zs are not interleaved.
        expected-value ["Y1" "Y2" "Z1" "Z2" "X1" "X2" "A" "B"]]
    (is (= expected-value (->value merged-crdt [] schema)))))

(deftest test-merge-multiple-conflicts
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        schema (l/array-schema l/string-schema)
        cmds0 [{:zeno/arg ["A" "B" "C"]
                :zeno/op :zeno/set
                :zeno/path []}]
        {crdt0 :crdt
         crdt0-ops :crdt-ops} (process-cmds cmds0 schema {:make-id make-id})
        cmds1 [{:zeno/arg "XF"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "XL"
                :zeno/op :zeno/insert-after
                :zeno/path [-1]}]
        cmds2 [{:zeno/arg "YF"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "YL"
                :zeno/op :zeno/insert-after
                :zeno/path [-1]}]
        cmds3 [{:zeno/arg "ZF"
                :zeno/op :zeno/insert-before
                :zeno/path [0]}
               {:zeno/arg "ZL"
                :zeno/op :zeno/insert-after
                :zeno/path [-1]}]
        {crdt1 :crdt
         crdt1-ops :crdt-ops} (process-cmds cmds1 schema
                                            {:crdt crdt0 :make-id make-id})
        {crdt2 :crdt
         crdt2-ops :crdt-ops} (process-cmds cmds2 schema
                                            {:crdt crdt0 :make-id make-id})
        {crdt3 :crdt
         crdt3-ops :crdt-ops} (process-cmds cmds3 schema
                                            {:crdt crdt0 :make-id make-id})
        new-crdt-ops (set/union crdt1-ops crdt2-ops crdt3-ops)
        merged-crdt (ops->crdt new-crdt-ops schema {:crdt crdt0})
        expected-value ["YF" "ZF" "XF" "A" "B" "C" "XL" "YL" "ZL"]]
    (is (= expected-value (->value merged-crdt [] schema)))))

(deftest test-nested-merge-conflict
  (let [schema pet-owner-schema
        ret0 (commands/process-cmds
              {:cmds [{:zeno/arg {:name "Bill"
                                  :pets [{:name "Pinky"
                                          :species "Felis catus"}
                                         {:name "Fishy"
                                          :species "Carassius auratus"}]}
                       :zeno/op :zeno/set
                       :zeno/path [root]}]
               :data-schema schema
               :root root
               :sys-time-ms (u/str->long "1640205282840")})
        {crdt0 :crdt crdt0-ops :crdt-ops} ret0
        ret1 (commands/process-cmds
              {:cmds [{:zeno/arg "Goldy"
                       :zeno/op :zeno/set
                       :zeno/path [root :pets -1 :name]}]
               :crdt crdt0
               :data-schema schema
               :root root
               :sys-time-ms (u/str->long "1640205282841")})
        ret2 (commands/process-cmds
              {:cmds [{:zeno/arg "Herman"
                       :zeno/op :zeno/set
                       :zeno/path [root :pets -1 :name]}]
               :crdt crdt0
               :data-schema schema
               :root root
               :sys-time-ms (u/str->long "1640205282842")})
        {crdt1 :crdt crdt1-ops :crdt-ops} ret1
        {crdt2 :crdt crdt2-ops :crdt-ops} ret2
        all-ops (set/union crdt1-ops crdt2-ops)
        merged-crdt (ops->crdt all-ops schema {:crdt crdt0})]
    ;; We expect "Herman" because its sys-time-ms is later
    (is (= "Herman" (->value merged-crdt [:pets -1 :name] schema)))))

(deftest test-set-into-empty-array
  (let [cmds [{:zeno/arg "1"
               :zeno/op :zeno/set
               :zeno/path []}]]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"must be sequential"
         (process-cmds cmds (l/array-schema l/string-schema))))))

(comment (krun #'test-set-map-of-two-arrays))
(deftest test-set-map-of-two-arrays
  (let [value {"a" [1] "b" [2]}
        schema (l/map-schema (l/array-schema l/int-schema))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-set-nested-map-of-two-arrays))
(deftest test-set-nested-map-of-two-arrays
  (let [value {"a" {"aa" [1]}
               "b" {"bb" [2]}}
        schema (l/map-schema (l/map-schema (l/array-schema l/int-schema)))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-arrays-set {:capture-output? true}))
(deftest test-crdt-nested-arrays-set
  (let [value [[[[[1 2]]]] [[[[3]] [[4]]]] [[[[5]]] [[[6 7]]]]]
        schema (-> l/int-schema l/array-schema l/array-schema
                   l/array-schema l/array-schema l/array-schema)
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-arrays-set-index))
(deftest test-crdt-nested-arrays-set-index
  (let [schema (l/array-schema (l/array-schema l/int-schema))
        cmds [{:zeno/arg [[1 2] [2]]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg [3]
               :zeno/op :zeno/set
               :zeno/path [1]}]
        expected-value [[1 2] [3]]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-crdt-nested-arrays-set-index-out-of-bounds))
(deftest test-crdt-nested-arrays-set-index-out-of-bounds
  (let [cmds [{:zeno/arg [[1 2]]
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/arg [3]
               :zeno/op :zeno/set
               :zeno/path [1]}]]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Index .* into array .* is out of bounds"
         (process-cmds cmds (l/array-schema (l/array-schema l/int-schema)))))))

(comment (krun #'test-set-nested-map-of-two-nested-arrays))
(deftest test-set-nested-map-of-two-nested-arrays
  (let [value {"a" {"aa" [[1 2] [3]]}
               "b" {"bb" [[4 5] [6]]}}
        schema (-> l/int-schema l/array-schema l/array-schema
                   l/map-schema l/map-schema)
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-set-remove-nested-arrays))
(deftest test-set-remove-nested-arrays
  (let [value [[1 2] [3]]
        schema (l/array-schema (l/array-schema l/int-schema))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}
              {:zeno/op :zeno/remove
               :zeno/path [0 1]}
              {:zeno/op :zeno/remove
               :zeno/path [1]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value [[1]]]
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array))
(deftest test-empty-array
  (let [value []
        schema (l/array-schema l/string-schema)
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array-as-value))
(deftest test-empty-array-as-value
  (let [value {"a" []}
        schema (l/map-schema (l/array-schema l/string-schema))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(comment (krun #'test-empty-array-in-record))
(deftest test-empty-array-in-record
  (let [value []
        schema (l/record-schema
                :r1 [[:a (l/array-schema l/string-schema)]])
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path [:a]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= value
           (->value crdt [:a] schema)
           (->value acrdt [:a] schema)))))

(comment (krun #'test-empty-array-in-map))
(deftest test-empty-array-in-map
  (let [value []
        schema (l/map-schema (l/array-schema l/string-schema))
        cmds [{:zeno/arg value
               :zeno/op :zeno/set
               :zeno/path ["a"]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
        cmds [{:zeno/arg value
               :zeno/op :zeno/insert-after
               :zeno/path [:a -1]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
        cmds [{:zeno/arg value
               :zeno/op :zeno/insert-after
               :zeno/path [-1]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
        cmds [{:zeno/arg value
               :zeno/op :zeno/insert-after
               :zeno/path [0 -1]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
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
        schema (l/map-schema (l/map-schema (l/array-schema l/int-schema)))
        cmds [{:zeno/arg value
               :zeno/op :zeno/insert-after
               :zeno/path ["a" "b" 0]}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)
        expected-value {"a" {"b" [value]}}]
    (is (= value
           (->value crdt ["a" "b" 0] schema)
           (->value acrdt ["a" "b" 0] schema)))
    (is (= expected-value
           (->value crdt [] schema)
           (->value acrdt [] schema)))))

(deftest test-punch-through-insert
  (let [data-schema (l/array-schema (l/array-schema l/int-schema))
        path [root]
        cmds [{:zeno/arg 1
               :zeno/op :zeno/insert-before
               :zeno/path [root 0 0]}]
        {:keys [crdt]} (commands/process-cmds (u/sym-map cmds data-schema root))
        v (get/get-in-state (u/sym-map crdt data-schema path root))]
    (is (= [[1]] v))))

(comment (krun #'test-deep-nested-negative-insert-after))
(deftest test-deep-nested-negative-insert-after
  (let [value "id"
        data-schema (l/record-schema
                     :r1 [[:a (l/map-schema
                               (l/array-schema
                                (l/map-schema
                                 (l/record-schema
                                  :r2 [[:d (l/array-schema
                                            (l/array-schema
                                             l/string-schema))]]))))]])
        cmds [{:zeno/arg value
               :zeno/op :zeno/insert-after
               :zeno/path [root :a "b" 0 "c" :d 0 -1]}]
        {:keys [crdt crdt-ops]} (commands/process-cmds
                                 (u/sym-map cmds data-schema root))
        acrdt (ops->crdt crdt-ops data-schema)
        expected-value {:a {"b" [{"c" {:d [[value]]}}]}}]
    (is (= value
           (->value crdt [:a "b" 0 "c" :d 0 -1] data-schema)
           (->value acrdt [:a "b" 0 "c" :d 0 -1] data-schema)))
    (is (= expected-value
           (->value crdt [] data-schema)
           (->value acrdt [] data-schema)))))

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
        cmds [{:zeno/arg data
               :zeno/op :zeno/set
               :zeno/path []}]
        {:keys [crdt crdt-ops]} (process-cmds cmds schema)
        acrdt (ops->crdt crdt-ops schema)]
    (is (= data
           (->value crdt [] schema)
           (->value acrdt [] schema)))))
