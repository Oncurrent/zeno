(ns unit.client-commands-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.state-providers.client-mem.commands :as commands]
   [com.oncurrent.zeno.utils :as u]
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
                          :value "b"}
        _ (is (= expected-get-ret get-ret))
        set-path [:zeno/client :some-stuff -1]
        set-ret (commands/eval-cmd state {:zeno/path set-path
                                          :zeno/op :zeno/set
                                          :zeno/arg {:name "new"}}
                                   :zeno/client)
        set-expected {:state {:some-stuff [{:name "a"}
                                           {:name "new"}]}}]
    (is (= set-expected set-ret))))

(deftest test-insert-after
  (let [state [:a :b]
        cmd {:zeno/arg :new
             :zeno/op :zeno/insert-after
             :zeno/path [:zeno/client -1]}
        ret (commands/eval-cmd state cmd :zeno/client)
        expected {:state [:a :b :new]}]
    (is (= expected ret))))

(deftest test-insert-before
  (let [state [:a :b]
        cmd {:zeno/arg :new
             :zeno/op :zeno/insert-before
             :zeno/path [:zeno/client -1]}
        ret (commands/eval-cmd state cmd :zeno/client)
        expected {:state [:a :new :b]}]
    (is (= expected ret))))

(deftest test-simple-remove-root
  (let [state [:a :b :c]
        cmd {:zeno/op :zeno/remove
             :zeno/path [:zeno/client -1]}
        ret (commands/eval-cmd state cmd :zeno/client)
        expected {:state [:a :b]}]
    (is (= expected ret))))

(deftest test-simple-remove-no-root
  (let [state [:a :b :c]
        path [:zeno/client -1]
        ret (commands/eval-cmd state
                               {:zeno/path path :zeno/op :zeno/remove}
                               :zeno/client)
        expected {:state [:a :b]}]
    (is (= expected ret))))

(deftest test-simple-insert*
  (let [state [:a :b]
        cases [[[:a :b :new] {:zeno/path [:zeno/client -1]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client -1]
                              :zeno/op :zeno/insert-before
                              :zeno/arg :new}]
               [[:new :a :b] {:zeno/path [:zeno/client 0]
                              :zeno/op :zeno/insert-before
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client 0]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 1]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client 1]
                              :zeno/op :zeno/insert-before
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 2]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :b :new] {:zeno/path [:zeno/client 10]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client -10]
                              :zeno/op :zeno/insert-after
                              :zeno/arg :new}]
               [[:a :new :b] {:zeno/path [:zeno/client 10]
                              :zeno/op :zeno/insert-before
                              :zeno/arg :new}]
               [[:new :a :b] {:zeno/path [:zeno/client -10]
                              :zeno/op :zeno/insert-before
                              :zeno/arg :new}]]]
    (doseq [case cases]
      (let [[expected cmd] case
            ret (:state (commands/eval-cmd state cmd :zeno/client))]
        (is (= case [ret cmd]))))))

(deftest test-deep-insert*
  (let [state {:x [:a :b]}
        cases [[{:x [:a :b :new]} {:zeno/path [:zeno/client :x -1]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:zeno/client :x -1]
                                   :zeno/op :zeno/insert-before
                                   :zeno/arg :new}]
               [{:x [:new :a :b]} {:zeno/path [:zeno/client :x 0]
                                   :zeno/op :zeno/insert-before
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:zeno/client :x 0]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:zeno/client :x 1]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:zeno/client :x 1]
                                   :zeno/op :zeno/insert-before
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:zeno/client :x 2]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :b :new]} {:zeno/path [:zeno/client :x 10]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:zeno/client :x -10]
                                   :zeno/op :zeno/insert-after
                                   :zeno/arg :new}]
               [{:x [:a :new :b]} {:zeno/path [:zeno/client :x 10]
                                   :zeno/op :zeno/insert-before
                                   :zeno/arg :new}]
               [{:x [:new :a :b]} {:zeno/path [:zeno/client :x -10]
                                   :zeno/op :zeno/insert-before
                                   :zeno/arg :new}]]]
    (doseq [case cases]
      (let [[expected cmd] case
            ret (:state (commands/eval-cmd state cmd :zeno/client))]
        (is (= case [ret cmd]))))))

(deftest test-bad-insert*-on-map
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"does not point to a sequence"
       (commands/eval-cmd {:zeno/client {}}
                          {:zeno/path [:zeno/client 0]
                           :zeno/op :zeno/insert-after
                           :zeno/arg :new}
                          :zeno/client))))

(deftest test-bad-insert*-path
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"the last element of the path must be an integer"
       (commands/eval-cmd {:zeno/client {}}
                          {:zeno/path [:zeno/client]
                           :zeno/op :zeno/insert-after
                           :zeno/arg :new}
                          :zeno/client))))

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
                                           {:zeno/path path :zeno/op :zeno/remove}
                                           :zeno/client))]
        (is (= case [ret state* path]))))))
