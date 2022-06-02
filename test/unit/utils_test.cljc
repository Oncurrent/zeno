(ns unit.utils-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is are]]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj [kaocha.repl])
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

(deftest test-chop-root
  (let [f u/chop-root]
    (is (= [] (f [] nil)))
    (is (= [] (f [:zeno/crdt] :zeno/crdt)))
    (is (= [:pigs] (f [:zeno/crdt :pigs] :zeno/crdt)))
    (is (= [:zeno/crdt :pigs] (f [:zeno/crdt :pigs] :pigs)))
    (is (= [:pigs] (f [:zeno/crdt :zeno/crdt :zeno/crdt :pigs] :zeno/crdt)))))

(comment
 (kaocha.repl/run #'test-fill-env-defaults {:color? false}))
(deftest test-fill-env-defaults
  (let [f u/fill-env-defaults]
    (is (= #:zeno{:env-name u/default-env-name
                  :source-env-name u/default-env-name
                  :env-lifetime-mins u/default-env-lifetime-mins}
           (f {})))
    (is (= #:zeno{:env-name "nacho"
                  :source-env-name u/default-env-name
                  :env-lifetime-mins 1}
           (f #:zeno{:env-name "nacho"
                     :env-lifetime-mins 1})))
    (let [ret (f #:zeno{:source-env-name "nacho"})]
      (is (= "nacho" (:zeno/source-env-name ret)))
      (is (= u/default-env-lifetime-mins (:zeno/env-lifetime-mins ret)))
      (is (= 26 (count (:zeno/env-name ret)))))
    (let [ret (f #:zeno{:env-lifetime-mins 1})]
      (is (= u/default-env-name (:zeno/source-env-name ret)))
      (is (= 1 (:zeno/env-lifetime-mins ret)))
      (is (= 26 (count (:zeno/env-name ret)))))))
