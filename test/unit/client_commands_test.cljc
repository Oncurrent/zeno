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
        get-path [:client :some-stuff -1 :name]
        get-ret (commands/get-in-state state get-path :client)
        expected-get-ret {:norm-path [:client :some-stuff 1 :name]
                          :val "b"}
        _ (is (= expected-get-ret get-ret))
        set-path [:client :some-stuff -1]
        set-ret (commands/eval-cmd state {:path set-path
                                          :op :set
                                          :arg {:name "new"}}
                                   :client)
        set-expected {:state {:some-stuff [{:name "a"}
                                           {:name "new"}]},
                      :update-info {:norm-path [:client :some-stuff 1]
                                    :op :set
                                    :value {:name "new"}}}]
    (is (= set-expected set-ret))))

(deftest test-insert-after
  (let [state [:a :b]
        path [:client -1]
        arg :new
        ret (commands/insert* state path :client :insert-after arg)
        expected {:state [:a :b :new],
                  :update-info {:norm-path [:client 2]
                                :op :insert-after
                                :value :new}}]
    (is (= expected ret))))

(deftest test-insert-before
  (let [state [:a :b]
        path [:client -1]
        arg :new
        ret (commands/insert* state path :client :insert-before arg)
        expected {:state [:a :new :b],
                  :update-info {:norm-path [:client 1]
                                :op :insert-before
                                :value :new}}]
    (is (= expected ret))))

(deftest test-simple-remove-prefix
  (let [state [:a :b :c]
        path [:client -1]
        ret (commands/eval-cmd state {:path path :op :remove} :client)
        expected {:state [:a :b],
                  :update-info {:norm-path [:client 2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-remove-no-prefix
  (let [state [:a :b :c]
        path [-1]
        ret (commands/eval-cmd state {:path path :op :remove} nil)
        expected {:state [:a :b],
                  :update-info {:norm-path [2]
                                :op :remove
                                :value nil}}]
    (is (= expected ret))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] {:path [:client -1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [:client -1]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [:client 0]
                              :op :insert-before
                              :arg :new}]
               [[:a :new :b] {:path [:client 0]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:client 1]
                              :op :insert-after
                              :arg :new}]
               [[:a :new :b] {:path [:client 1]
                              :op :insert-before
                              :arg :new}]
               [[:a :b :new] {:path [:client 2]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:client 10]
                              :op :insert-after
                              :arg :new}]
               [[:new :a :b] {:path [:client -10]
                              :op :insert-after
                              :arg :new}]
               [[:a :b :new] {:path [:client 10]
                              :op :insert-before
                              :arg :new}]
               [[:new :a :b] {:path [:client -10]
                              :op :insert-before
                              :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path :client op arg))]
        (is (= case [ret cmd]))))))

(deftest test-deep-insert*
  (let [state {:x [:a :b]}
        cases [[{:x [:a :b :new]} {:path [:x -1]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x -1]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x 0]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x 0]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 1]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :new :b]} {:path [:x 1]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 2]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 10]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x -10]
                                   :op :insert-after
                                   :arg :new}]
               [{:x [:a :b :new]} {:path [:x 10]
                                   :op :insert-before
                                   :arg :new}]
               [{:x [:new :a :b]} {:path [:x -10]
                                   :op :insert-before
                                   :arg :new}]]]
    (doseq [case cases]
      (let [[expected {:keys [path op arg] :as cmd}] case
            ret (:state (commands/insert* state path nil op arg))]
        (is (= case [ret cmd]))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a sequence"
       (commands/insert* {:local {}} [:local 0] :local :insert-before :new))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (commands/insert* [] [] :local :insert-before :new))))

(deftest test-remove
  (let [state {:x [:a :b :c]}
        cases [[{:w 1} {:w 1 :z 2} [:local :z]]
               [{:w 1 :z {:b 2}} {:w 1 :z {:a 1 :b 2}} [:local :z :a]]
               [{:x [:b :c]} state [:local :x 0]]
               [{:x [:a :c]} state [:local :x 1]]
               [{:x [:a :b]} state [:local :x 2]]
               [{:x [:a :b]} state [:local :x -1]]
               [{:x [:a :c]} state [:local :x -2]]]]
    (doseq [case cases]
      (let [[expected state* path] case
            ret (:state (commands/eval-cmd state*
                                           {:path path :op :remove} :local))]
        (is (= case [ret state* path]))))))

(deftest test-math-no-prefix
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:path [:a] :op :+ :arg 1}]
               [{:a 9} state0 {:path [:a] :op :- :arg 1}]
               [{:a 20} state0 {:path [:a] :op :* :arg 2}]
               [{:a 5} state0 {:path [:a] :op :/ :arg 2}]
               [{:a 1} state0 {:path [:a] :op :mod :arg 3}]
               [{:x {:a 11}} state1 {:path [:x :a] :op :+ :arg 1}]
               [{:x {:a 9}} state1 {:path [:x :a] :op :- :arg 1}]
               [{:x {:a 30}} state1 {:path [:x :a] :op :* :arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:path [:x :a] :op :/ :arg 3}]
               [{:x {:a 1}} state1 {:path [:x :a] :op :mod :arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:path [:x 1 :a] :op :+ :arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:path [:x 1 :a] :op :- :arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (:state (commands/eval-cmd state* cmd nil))]
        (is (= case [ret state* cmd]))))))

(deftest test-math-local-prefix
  (let [state0 {:a 10}
        state1 {:x {:a 10}}
        state2 {:x [{:a 10} {:a 20}]}
        cases [[{:a 11} state0 {:path [:local :a] :op :+ :arg 1}]
               [{:a 9} state0 {:path [:local :a] :op :- :arg 1}]
               [{:a 20} state0 {:path [:local :a] :op :* :arg 2}]
               [{:a 5} state0 {:path [:local :a] :op :/ :arg 2}]
               [{:a 1} state0 {:path [:local :a] :op :mod :arg 3}]
               [{:x {:a 11}} state1 {:path [:local :x :a] :op :+ :arg 1}]
               [{:x {:a 9}} state1 {:path [:local :x :a] :op :- :arg 1}]
               [{:x {:a 30}} state1 {:path [:local :x :a] :op :* :arg 3}]
               [{:x {:a (/ 10 3)}} state1 {:path [:local :x :a] :op :/ :arg 3}]
               [{:x {:a 1}} state1 {:path [:local :x :a] :op :mod :arg 3}]
               [{:x [{:a 10} {:a 21}]} state2 {:path [:local :x 1 :a] :op :+ :arg 1}]
               [{:x [{:a 10} {:a 19}]} state2 {:path [:local :x 1 :a] :op :- :arg 1}]]]
    (doseq [case cases]
      (let [[expected state* cmd] case
            ret (:state (commands/eval-cmd state* cmd :local))]
        (is (= case [ret state* cmd]))))))
