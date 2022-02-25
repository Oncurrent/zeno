(ns unit.client-commands-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [oncurrent.zeno.client.client-commands :as commands]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-commands-get-set
  (let [state {:some-stuff [{:name "a"}
                            {:name "b"}]}
        get-path [:zeno/client :some-stuff -1 :name]
        get-ret (commands/get-in-state state get-path :zeno/client)
        expected-get-ret {:norm-path [:zeno/client :some-stuff 1 :name]
                          :val "b"}
        _ (is (= expected-get-ret get-ret))
        set-path [:zeno/client :some-stuff -1]
        set-ret (commands/eval-cmd state {:zeno/path set-path
                                          :zeno/op :set
                                          :zeno/arg {:name "new"}}
                                   :zeno/client)
        set-expected {:state {:some-stuff [{:name "a"}
                                           {:name "new"}]},
                      :update-info {:norm-path [:zeno/client :some-stuff 1]
                                    :op :set
                                    :value {:name "new"}}}]
    (is (= set-expected set-ret))))

(deftest test-insert-after
  (let [state [:a :b]
        path [:zeno/client -1]
        arg :new
        ret (commands/insert* state path :zeno/client :insert-after arg)
        expected {:state [:a :b :new],
                  :update-info {:norm-path [:zeno/client 2]
                                :op :insert-after
                                :value :new}}]
    (is (= expected ret))))

(deftest test-insert-before
  (let [state [:a :b]
        path [:zeno/client -1]
        arg :new
        ret (commands/insert* state path :zeno/client :insert-before arg)
        expected {:state [:a :new :b],
                  :update-info {:norm-path [:zeno/client 1]
                                :op :insert-before
                                :value :new}}]
    (is (= expected ret))))

(deftest test-simple-remove-prefix
  (let [state [:a :b :c]
        path [:zeno/client -1]
        ret (commands/eval-cmd state {:zeno/path path :zeno/op :remove} :zeno/client)
        expected {:state [:a :b],
                  :update-info {:norm-path [:zeno/client 2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-remove-no-prefix
  (let [state [:a :b :c]
        path [-1]
        ret (commands/eval-cmd state {:zeno/path path :zeno/op :remove} nil)
        expected {:state [:a :b],
                  :update-info {:norm-path [2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] {:zeno/path [:zeno/client -1]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client -1]
                              :zeno/op :insert-before
                              :zeno/arg :new}]
               [[:new :a :b] {:zeno/path [:zeno/client 0]
                              :zeno/op :insert-before
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client 0]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 1]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client 1]
                              :zeno/op :insert-before
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 2]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 10]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:new :a :b] {:zeno/path [:zeno/client -10]
                              :zeno/op :insert-after
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 10]
                              :zeno/op :insert-before
                              :zeno/arg :new}]
               [[:new :a :b] {:zeno/path [:zeno/client -10]
                              :zeno/op :insert-before
                              :zeno/arg :new}]]]
    (doseq [case cases]
      (let [[expected {:zeno/keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path :zeno/client op arg))]
        (is (= case [ret cmd]))))))

(deftest test-deep-insert*
  (let [state {:x [:a :b]}
        cases [[{:x [:a :b :new]} {:zeno/path [:x -1]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:x -1]
                                   :zeno/op :insert-before
                                   :zeno/arg :new}]
               [{:x [:new :a :b]} {:zeno/path [:x 0]
                                   :zeno/op :insert-before
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:x 0]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:x 1]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:x 1]
                                   :zeno/op :insert-before
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:x 2]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:x 10]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:new :a :b]} {:zeno/path [:x -10]
                                   :zeno/op :insert-after
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:x 10]
                                   :zeno/op :insert-before
                                   :zeno/arg :new}]
               [{:x [:new :a :b]} {:zeno/path [:x -10]
                                   :zeno/op :insert-before
                                   :zeno/arg :new}]]]
    (doseq [case cases]
      (let [[expected {:zeno/keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path nil op arg))]
        (is (= case [ret cmd]))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a sequence"
       (commands/insert* {:zeno/client {}}
                         [:zeno/client 0]
                         :zeno/client
                         :insert-before
                         :new))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (commands/insert* [] [] :zeno/client :insert-before :new))))

(deftest test-remove
  (let [state {:x [:a :b :c]}
        cases [[{:w 1} {:w 1 :z 2} [:zeno/client :z]]
               [{:w 1 :z {:b 2}} {:w 1 :z {:a 1 :b 2}} [:zeno/client :z :a]]
               [{:x [:b :c]} state [:zeno/client :x 0]]
               [{:x [:a :c]} state [:zeno/client :x 1]]
               [{:x [:a :b]} state [:zeno/client :x 2]]
               [{:x [:a :b]} state [:zeno/client :x -1]]
               [{:x [:a :c]} state [:zeno/client :x -2]]]]
    (doseq [case cases]
      (let [[expected state* path] case
            ret (:state (commands/eval-cmd state*
                                           {:zeno/path path :zeno/op :remove} :zeno/client))]
        (is (= case [ret state* path]))))))

(deftest test-math-no-prefix
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:zeno/path [:a] :zeno/op :+ :zeno/arg 1}]
               [{:a 9} state0 {:zeno/path [:a] :zeno/op :- :zeno/arg 1}]
               [{:a 20} state0 {:zeno/path [:a] :zeno/op :* :zeno/arg 2}]
               [{:a 5} state0 {:zeno/path [:a] :zeno/op :/ :zeno/arg 2}]
               [{:a 1} state0 {:zeno/path [:a] :zeno/op :mod :zeno/arg 3}]
               [{:x {:a 11}} state1 {:zeno/path [:x :a] :zeno/op :+ :zeno/arg 1}]
               [{:x {:a 9}} state1 {:zeno/path [:x :a] :zeno/op :- :zeno/arg 1}]
               [{:x {:a 30}} state1 {:zeno/path [:x :a] :zeno/op :* :zeno/arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:zeno/path [:x :a] :zeno/op :/ :zeno/arg 3}]
               [{:x {:a 1}} state1 {:zeno/path [:x :a] :zeno/op :mod :zeno/arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:zeno/path [:x 1 :a] :zeno/op :+ :zeno/arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:zeno/path [:x 1 :a] :zeno/op :- :zeno/arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (:state (commands/eval-cmd state* cmd nil))]
        (is (= case [ret state* cmd]))))))

(deftest test-math-local-prefix
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:zeno/path [:zeno/client :a] :zeno/op :+ :zeno/arg 1}]
               [{:a 9} state0 {:zeno/path [:zeno/client :a] :zeno/op :- :zeno/arg 1}]
               [{:a 20} state0 {:zeno/path [:zeno/client :a] :zeno/op :* :zeno/arg 2}]
               [{:a 5} state0 {:zeno/path [:zeno/client :a] :zeno/op :/ :zeno/arg 2}]
               [{:a 1} state0 {:zeno/path [:zeno/client :a] :zeno/op :mod :zeno/arg 3}]
               [{:x {:a 11}} state1 {:zeno/path [:zeno/client :x :a] :zeno/op :+ :zeno/arg 1}]
               [{:x {:a 9}} state1 {:zeno/path [:zeno/client :x :a] :zeno/op :- :zeno/arg 1}]
               [{:x {:a 30}} state1 {:zeno/path [:zeno/client :x :a] :zeno/op :* :zeno/arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:zeno/path [:zeno/client :x :a] :zeno/op :/ :zeno/arg 3}]
               [{:x {:a 1}} state1 {:zeno/path [:zeno/client :x :a] :zeno/op :mod :zeno/arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:zeno/path [:zeno/client :x 1 :a] :zeno/op :+ :zeno/arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:zeno/path [:zeno/client :x 1 :a] :zeno/op :- :zeno/arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (:state (commands/eval-cmd state* cmd :zeno/client))]
        (is (= case [ret state* cmd]))))))
