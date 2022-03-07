(ns unit.utils-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is are]]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log]))

(deftest test-sub-map->map-info
  (let [sub-map '{c [:zeno/client :b-to-c b]
                  b [:zeno/client :a-to-b a]
                  a [:zeno/client :a]
                  x [:zeno/client x]
                  z dynamic-path}
        resolution-map '{x :foo
                         dynamic-path [:zeno/client :dp]}
        info (u/sub-map->map-info sub-map resolution-map)
        {:keys [independent-pairs ordered-dependent-pairs]} info
        expected-independent-pairs-set (set '[[x [:zeno/client :foo]]
                                              [a [:zeno/client :a]]
                                              [z [:zeno/client :dp]]]),
        expected-ordered-dependent-pairs '[[b [:zeno/client :a-to-b a]]
                                           [c [:zeno/client :b-to-c b]]]]
    (is (= expected-ordered-dependent-pairs ordered-dependent-pairs))
    (is (= expected-independent-pairs-set
           (set independent-pairs)))))

(deftest test-relationship
  (are [ret ks1 ks2] (= ret (u/relationship-info ks1 ks2))
    [:equal nil] [] []
    [:equal nil]  [:a] [:a]
    [:equal nil] [:a :b :c] [:a :b :c]
    [:parent [:a]] [] [:a]
    [:parent [:b]] [:a] [:a :b]
    [:parent [:b :c]] [:a] [:a :b :c]
    [:parent [:c]] [:a :b] [:a :b :c]
    [:child nil][:a] []
    [:child nil] [:a :b] [:a]
    [:child nil] [:a :b :c :d] [:a :b]
    [:sibling nil] [:a] [:b]
    [:sibling nil] [:a :c] [:b :c]
    [:sibling nil] [:b] [:c :d :e]
    [:sibling nil] [:a :b] [:a :c]
    [:sibling nil] [:a :b] [:b :c]
    [:sibling nil] [:a :b] [:a :c]
    [:sibling nil] [:a :c :d] [:a :b :d]))

(deftest test-expand-path-1
  (try
    (let [path [:x [:a :b]]
          expected [[:x :a]
                    [:x :b]]]
      (is (= expected (u/expand-path (constantly nil) path))))
    (catch #?(:clj Exception :cljs js/Error) e
      (is (= :unexpected e)))))

(deftest test-expand-path-2
  (try
    (let [path [:b [1 2] :c [3 5] :d]
          expected [[:b 1 :c 3 :d]
                    [:b 2 :c 3 :d]
                    [:b 1 :c 5 :d]
                    [:b 2 :c 5 :d]]]
      (is (= expected (u/expand-path (constantly nil) path))))
    (catch #?(:clj Exception :cljs js/Error) e
      (is (= :unexpected e)))))

(deftest test-expand-path-3
  (try
    (let [path [[1 2][3 5] :d]
          expected [[1 3 :d]
                    [2 3 :d]
                    [1 5 :d]
                    [2 5 :d]]]
      (is (= expected (u/expand-path (constantly nil) path))))
    (catch #?(:clj Exception :cljs js/Error) e
      (is (= :unexpected e)))))

(deftest test-expand-path-4
  (try
    (let [path [:x ["1" "2"] :y [:a :b]]
          expected
          [[:x "1" :y :a]
           [:x "2" :y :a]
           [:x "1" :y :b]
           [:x "2" :y :b]]]
      (is (= expected (u/expand-path (constantly nil) path))))
    (catch #?(:clj Exception :cljs js/Error) e
      (is (= :unexpected e)))))
