(ns unit.state-subscription-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.client.state-subscriptions :as state-subscriptions]
   [com.oncurrent.zeno.utils :as u]
   #?(:clj kaocha.repl)
   [taoensso.timbre :as log]
   [test-common :as c])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns*))

(deftest test-empty-sub-map
  (let [zc (c/->zc-unit)
        bad-sub-map {}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The `sub-map` argument must contain at least one entry"
         (zc/subscribe-to-state! zc "test" bad-sub-map
                                 (constantly true))))
    (zc/stop! zc)))

(deftest test-nil-sub-map
  (let [zc (c/->zc-unit)
        bad-sub-map nil]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"The `sub-map` argument must be a map"
         (zc/subscribe-to-state! zc "test" bad-sub-map
                                 (constantly true))))
    (zc/stop! zc)))

(deftest test-non-sym-key-in-sub-map
  (let [zc (c/->zc-unit)
        bad-sub-map {:not-a-symbol [:zeno/client :user-id]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Keys must be symbols"
         (zc/subscribe-to-state! zc "test" bad-sub-map
                                 (constantly true))))
    (zc/stop! zc)))

(deftest test-bad-path-in-sub-map
  (let [zc (c/->zc-unit)
        bad-sub-map '{user-id [:zeno/client 8.9]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Only integers"
         (zc/subscribe-to-state! zc "test" bad-sub-map
                                 (constantly true))))
    (zc/stop! zc)))

(deftest test-subscribe!
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           name "Alice"
           user-id "123"
           sub-map '{id [:zeno/client :user-id]
                     name [:zeno/client :users id :name]}
           expected '{id "123"
                      name "Alice"}]
       (is (= true
              (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :users]
                                             :zeno/op :zeno/set
                                             :zeno/arg {user-id {:name name}}}
                                            {:zeno/path [:zeno/client :user-id]
                                             :zeno/op :zeno/set
                                             :zeno/arg user-id}]))))
       (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                               update-fn)))
       (zc/stop! zc)))))

(comment (kaocha.repl/run #'test-subscribe!-actor-id))
(deftest test-subscribe!-actor-id
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)
           ch (ca/chan 1)
           update-fn #(ca/put! ch %)
           actor-id "AAAA"
           sub-map '{actor-id* [:zeno/actor-id]}
           expected '{actor-id* "AAAA"}]
       (reset! (:*actor-id zc) actor-id) ; Simulate login
       (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                               update-fn)))
       (zc/stop! zc)))))

(deftest test-subscribe!-single-entry
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)
           update-fn (constantly nil)
           name "Alice"
           user-id "123"
           sub-map '{id [:zeno/client :user-id]}]
       (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :user-id]
                                      :zeno/op :zeno/set
                                      :zeno/arg user-id}]))
       (is (= {'id user-id} (zc/subscribe-to-state! zc "test" sub-map
                                                    update-fn)))
       (zc/stop! zc)))))

(deftest test-bad-path-root-in-sub-map
  (let [zc (c/->zc-unit)
        sub-map '{a [:not-a-valid-root :x]}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must begin with "
         (zc/subscribe-to-state! zc "test" sub-map
                                 (constantly nil))))
    (zc/stop! zc)))

(deftest test-bad-path-root-in-sub-map-not-a-sequence
  (let [zc (c/->zc-unit)
        sub-map '{actor-id :zeno/actor-id}]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must be sequences"
         (zc/subscribe-to-state! zc "test" sub-map
                                 (constantly nil))))
    (zc/stop! zc)))

(deftest test-update-sub?-numeric
  (let [update-infos [{:norm-path [:sys 0]
                       :op :zeno/insert
                       :value "hi"}]
        sub-paths [[:zeno/client :page]]]
    (is (= false (state-subscriptions/update-sub? update-infos sub-paths)))))

(deftest test-order-by-lineage
  (let [*name->info (atom {"a" {}
                           "b" {:parents #{"a"}}
                           "c" {:parents #{"a" "b"}}
                           "d" {:parents #{"a"}}
                           "e" {}
                           "f" {:parents #{"e"}}
                           "g" {:parents #{"e"}}
                           "h" {:parents #{"e" "g"}}})]
    (is (= ["a"] (state-subscriptions/order-by-lineage
                  #{"a"} *name->info)))
    (is (= ["a" "b"] (state-subscriptions/order-by-lineage
                      #{"a" "b"} *name->info)))
    (is (= ["a" "b" "c"] (state-subscriptions/order-by-lineage
                          #{"a" "b" "c"} *name->info)))
    (is (= ["b" "c"] (state-subscriptions/order-by-lineage
                      #{"b" "c"} *name->info)))
    (is (= ["a" "b" "c" "d"] (state-subscriptions/order-by-lineage
                              #{"a" "b" "c" "d"} *name->info)))
    (is (= ["a" "b" "c" "e" "d"] (state-subscriptions/order-by-lineage
                                  #{"a" "b" "c" "d" "e"} *name->info)))
    (is (= ["e"] (state-subscriptions/order-by-lineage
                  #{"e"} *name->info)))
    (is (= ["e" "f"] (state-subscriptions/order-by-lineage
                      #{"e" "f"} *name->info)))
    (is (= ["h" "f"] (state-subscriptions/order-by-lineage
                      #{"h" "f"} *name->info)))))

(deftest test-get-state-and-expanded-paths
  (let [zc (c/->zc-unit {:initial-client-state {:page :frobnozzle}})
        independent-pairs [['page [:zeno/client :page]]
                           ['actor-id [:zeno/actor-id]]]
        ordered-dependent-pairs []
        ret (state-subscriptions/get-state-and-expanded-paths
             zc independent-pairs ordered-dependent-pairs)
        {:keys [state expanded-paths]} ret
        expected-state {'page :frobnozzle
                        'actor-id nil}
        expected-expanded-paths [[:zeno/client :page]
                                 [:zeno/actor-id]]
        _ (is (= expected-state state))
        _ (is (= expected-expanded-paths expanded-paths))
        actor-id2 "AAAA"
        _ (reset! (:*actor-id zc) actor-id2) ; Simulate login
        ret2 (state-subscriptions/get-state-and-expanded-paths
              zc independent-pairs ordered-dependent-pairs)
        expected-state2 {'page :frobnozzle
                         'actor-id actor-id2}
        expected-expanded-paths2 [[:zeno/client :page]
                                  [:zeno/actor-id]]]
    (is (= expected-state2 (:state ret2)))
    (is (= expected-expanded-paths2 (:expanded-paths ret2)))
    (zc/stop! zc)))
