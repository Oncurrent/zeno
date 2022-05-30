(ns unit.update-state-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.client :as zc]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log]
   [test-common :as c])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns*))

(deftest test-bad-path-root-in-update-state!
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Paths must begin with "
            (au/<? (zc/<update-state! zc [{:zeno/path [:not-a-valid-root :x]
                                           :zeno/op :zeno/set
                                           :zeno/arg 1}]))))
       (zc/stop! zc)))))

(deftest test-bad-command-op
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Invalid"
            (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :x]
                                           :zeno/op :not-an-op
                                           :zeno/arg 1}]))))
       (zc/stop! zc)))))

(deftest test-crdt-bad-command-op
  (au/test-async
   1000
   (ca/go
     (let [zc (c/->zc-unit)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Invalid"
            (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :x]
                                           :zeno/op :not-an-op
                                           :zeno/arg 1}]))))
       (zc/stop! zc)))))

(deftest test-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             sub-map '{text [:zeno/client :msgs 0 :text]}
             ch (ca/chan 1)
             update-fn #(ca/put! ch (% 'text))
             orig-text "Plato"
             new-text "Socrates"]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client]
                                  :zeno/op :zeno/set
                                  :zeno/arg {:msgs [{:text orig-text}]}}]))))
         (is (= {'text orig-text}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc
                                   [{:zeno/path [:zeno/client :msgs 0]
                                     :zeno/op :zeno/insert-before
                                     :zeno/arg {:text orig-text}}
                                    {:zeno/path [:zeno/client :msgs 0 :text]
                                     :zeno/op :zeno/set
                                     :zeno/arg new-text}]))
         (is (= new-text (au/<? ch)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :msgs 0 :text]
                                        :zeno/op :zeno/set
                                        :zeno/arg new-text}
                                       {:zeno/path [:zeno/client :msgs 0]
                                        :zeno/op :zeno/insert-before
                                        :zeno/arg {:text orig-text}}]))
         (is (= orig-text (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(comment
 (kaocha.repl/run #'test-ordered-update-maps {:capture-output? false})
 (kaocha.repl/run #'test-crdt-ordered-update-maps {:capture-output? false}))
(deftest test-crdt-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             sub-map '{text [:zeno/crdt :msgs 0 :text]}
             ch (ca/chan 1)
             update-fn #(ca/put! ch (% 'text))
             orig-text "Plato"
             new-text "Socrates"]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/crdt]
                                  :zeno/op :zeno/set
                                  :zeno/arg {:msgs [{:text orig-text}]}}]))))
         (is (= {'text orig-text}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc
                                   [{:zeno/path [:zeno/crdt :msgs 0]
                                     :zeno/op :zeno/insert-before
                                     :zeno/arg {:text orig-text}}
                                    {:zeno/path [:zeno/crdt :msgs 0 :text]
                                     :zeno/op :zeno/set
                                     :zeno/arg new-text}]))
         (is (= new-text (au/<? ch)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :msgs 0 :text]
                                        :zeno/op :zeno/set
                                        :zeno/arg new-text}
                                       {:zeno/path [:zeno/crdt :msgs 0]
                                        :zeno/op :zeno/insert-before
                                        :zeno/arg {:text orig-text}}]))
         (is (= orig-text (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-end-relative-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [orig-text "Foo"
             zc (c/->zc-unit)
             ch (ca/chan 1)
             sub-map '{last-text [:zeno/client :msgs -1 :text]}
             update-fn #(ca/put! ch (% 'last-text))
             new-text "Bar"
             ret (au/<? (zc/<update-state!
                         zc [{:zeno/path [:zeno/client]
                              :zeno/op :zeno/set
                              :zeno/arg {:msgs [{:text orig-text}]}}]))]
         (is (= true ret))
         (is (= {'last-text orig-text}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :msgs -1]
                                        :zeno/op :zeno/insert-after
                                        :zeno/arg {:text new-text}}]))
         (is (= new-text (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-end-relative-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [orig-text "Foo"
             zc (c/->zc-unit)
             ch (ca/chan 1)
             sub-map '{last-text [:zeno/crdt :msgs -1 :text]}
             update-fn #(ca/put! ch (% 'last-text))
             new-text "Bar"
             ret (au/<? (zc/<update-state!
                         zc [{:zeno/path [:zeno/crdt]
                              :zeno/op :zeno/set
                              :zeno/arg {:msgs [{:text orig-text}]}}]))]
         (is (= true ret))
         (is (= {'last-text orig-text}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :msgs -1]
                                        :zeno/op :zeno/insert-after
                                        :zeno/arg {:text new-text}}]))
         (is (= new-text (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-resolution-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'book-id book-id}
             sub-map '{title [:zeno/client :books book-id :title]}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :books book-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg {:title book-title}}]))))
         (is (= {'title book-title}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-resolution-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'book-id book-id}
             sub-map '{title [:zeno/crdt :books book-id :title]}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :books book-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg {:title book-title}}]))))
         (is (= {'title book-title}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-symbolic-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'d-path [:zeno/client :books 'the-id :title]}
             sub-map '{the-id [:zeno/client :the-id]
                       title d-path}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :the-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg book-id}
                                              {:zeno/path [:zeno/client :books book-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg {:title book-title}}]))))
         (is (= '{the-id "123"
                  title "Treasure Island"}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-symbolic-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             book-id "123"
             book-title "Treasure Island"
             resolution-map {'d-path [:zeno/crdt :books 'the-id :title]}
             sub-map '{the-id [:zeno/crdt :the-id]
                       title d-path}
             update-fn (constantly nil)]
         (is (= true
                (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :the-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg book-id}
                                              {:zeno/path [:zeno/crdt :books book-id]
                                               :zeno/op :zeno/set
                                               :zeno/arg {:title book-title}}]))))
         (is (= '{the-id "123"
                  title "Treasure Island"}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:zeno/client :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn (constantly nil)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :books]
                                                    :zeno/op :zeno/set
                                                    :zeno/arg books}]))))
         (is (= expected (zc/subscribe-to-state!
                          zc "test" sub-map update-fn
                          (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:zeno/crdt :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn (constantly nil)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :books]
                                                    :zeno/op :zeno/set
                                                    :zeno/arg books}]))))
         (is (= expected (zc/subscribe-to-state!
                          zc "test" sub-map update-fn
                          (u/sym-map resolution-map))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             update-ch (ca/chan 1)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/client :my-book-ids]
                       my-books [:zeno/client :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             expected {'my-book-ids my-book-ids
                       'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client :books]
                                  :zeno/op :zeno/set
                                  :zeno/arg books}
                                 {:zeno/path [:zeno/client :my-book-ids]
                                  :zeno/op :zeno/set
                                  :zeno/arg my-book-ids}]))))
         (is (= expected (zc/subscribe-to-state! zc "test"
                                                 sub-map update-fn)))
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client :my-book-ids 0]
                                  :zeno/op :zeno/remove}]))))
         (is (= {'my-book-ids ["456"]
                 'my-books [{:title "Kidnapped"}]} (au/<? update-ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-sequence-join-in-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             update-ch (ca/chan 1)
             my-book-ids ["123" "456"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/crdt :my-book-ids]
                       my-books [:zeno/crdt :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             expected {'my-book-ids my-book-ids
                       'my-books (vals (select-keys books my-book-ids))}]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/crdt :books]
                                  :zeno/op :zeno/set
                                  :zeno/arg books}
                                 {:zeno/path [:zeno/crdt :my-book-ids]
                                  :zeno/op :zeno/set
                                  :zeno/arg my-book-ids}]))))
         (is (= expected (zc/subscribe-to-state! zc "test"
                                                 sub-map update-fn)))
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/crdt :my-book-ids 0]
                                  :zeno/op :zeno/remove}]))))
         (is (= {'my-book-ids ["456"]
                 'my-books [{:title "Kidnapped"}]} (au/<? update-ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map-2
  ;; Tests changing state whose path depends on a different path in sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             update-ch (ca/chan 1)
             my-book-ids ["123" "789"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/client :my-book-ids]
                       my-books [:zeno/client :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             new-title "Strange Case of Dr Jekyll and Mr Hyde"
             expected1 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title "Dr Jekyll and Mr Hyde"}]}
             expected2 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title new-title}]}]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client :books]
                                  :zeno/op :zeno/set
                                  :zeno/arg books}
                                 {:zeno/path [:zeno/client :my-book-ids]
                                  :zeno/op :zeno/set
                                  :zeno/arg my-book-ids}]))))
         (is (= expected1 (zc/subscribe-to-state! zc "test" sub-map
                                                  update-fn)))
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client :books "789" :title]
                                  :zeno/op :zeno/set
                                  :zeno/arg new-title}]))))
         (is (= expected2 (au/<? update-ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-sequence-join-in-sub-map-2
  ;; Tests changing state whose path depends on a different path in sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             update-ch (ca/chan 1)
             my-book-ids ["123" "789"]
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/crdt :my-book-ids]
                       my-books [:zeno/crdt :books my-book-ids]}
             update-fn #(ca/put! update-ch %)
             new-title "Strange Case of Dr Jekyll and Mr Hyde"
             expected1 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title "Dr Jekyll and Mr Hyde"}]}
             expected2 {'my-book-ids my-book-ids
                        'my-books [{:title "Treasure Island"}
                                   {:title new-title}]}]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/crdt :books]
                                  :zeno/op :zeno/set
                                  :zeno/arg books}
                                 {:zeno/path [:zeno/crdt :my-book-ids]
                                  :zeno/op :zeno/set
                                  :zeno/arg my-book-ids}]))))
         (is (= expected1 (zc/subscribe-to-state! zc "test" sub-map
                                                  update-fn)))
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/crdt :books "789" :title]
                                  :zeno/op :zeno/set
                                  :zeno/arg new-title}]))))
         (is (= expected2 (au/<? update-ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-set-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             my-book-ids #{"123" "789"}
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:zeno/client :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn #(ca/put! ch %)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= {'my-books [nil nil]}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (is (= true (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :books]
                                                    :zeno/op :zeno/set
                                                    :zeno/arg books}]))))
         (is (= expected (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-set-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             my-book-ids #{"123" "789"}
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-books [:zeno/crdt :books my-book-ids]}
             resolution-map {'my-book-ids my-book-ids}
             update-fn #(ca/put! ch %)
             expected {'my-books (vals (select-keys books my-book-ids))}]
         (is (= {'my-books [nil nil]}
                (zc/subscribe-to-state! zc "test" sub-map update-fn
                                        (u/sym-map resolution-map))))
         (is (= true (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :books]
                                                    :zeno/op :zeno/set
                                                    :zeno/arg books}]))))
         (is (= expected (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-kw-operators
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island" :nums [2 4 6]}
                    "456" {:title "Kidnapped" :nums [1 3]}
                    "789" {:title "Dr Jekyll and Mr Hyde" :nums [5 7]}}
             msgs [{:text "hi" :user-id "123"}
                   {:text "there" :user-id "123"}]
             titles-set (set (map :title (vals books)))
             sub-map '{books-map [:zeno/client :books]
                       books-vals [:zeno/client :books :zeno/*]
                       titles-1 [:zeno/client :books :zeno/* :title]
                       book-ids [:zeno/client :books :zeno/keys]
                       titles-2 [:zeno/client :books book-ids :title]
                       num-books [:zeno/client :books :zeno/count]
                       num-books-2 [:zeno/client :books :zeno/* :zeno/count]
                       num-msgs [:zeno/client :msgs :zeno/count]
                       book-nums [:zeno/client :books :zeno/* :nums :zeno/concat]
                       msgs [:zeno/client :msgs]
                       msg-indices [:zeno/client :msgs :zeno/keys]}
             update-fn (constantly nil)
             expected {'book-ids #{"123" "456" "789"}
                       'books-map books
                       'books-vals (set (vals books))
                       'num-books (count books)
                       'num-books-2 (count books)
                       'book-nums #{2 4 6 1 3 5 7}
                       'titles-1 titles-set
                       'titles-2 titles-set
                       'num-msgs 2
                       'msg-indices [0 1]
                       'msgs msgs}
             update-ret (au/<? (zc/<update-state!
                                zc [{:zeno/path [:zeno/client]
                                     :zeno/op :zeno/set
                                     :zeno/arg (u/sym-map books msgs)}]))
             sub-ret (zc/subscribe-to-state! zc "test" sub-map
                                             update-fn)]
         (is (= true update-ret))
         (is (= expected
                (-> sub-ret
                    (update 'book-ids set)
                    (update 'books-vals set)
                    (update 'book-nums set)
                    (update 'titles-1 set)
                    (update 'titles-2 set))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e))
         (println (u/ex-msg-and-stacktrace e)))))))

(deftest test-crdt-kw-operators
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island" :nums [2 4 6]}
                    "456" {:title "Kidnapped" :nums [1 3]}
                    "789" {:title "Dr Jekyll and Mr Hyde" :nums [5 7]}}
             msgs [{:text "hi" :user-id "123"}
                   {:text "there" :user-id "123"}]
             titles-set (set (map :title (vals books)))
             sub-map '{books-map [:zeno/crdt :books]
                       books-vals [:zeno/crdt :books :zeno/*]
                       titles-1 [:zeno/crdt :books :zeno/* :title]
                       book-ids [:zeno/crdt :books :zeno/keys]
                       titles-2 [:zeno/crdt :books book-ids :title]
                       num-books [:zeno/crdt :books :zeno/count]
                       num-books-2 [:zeno/crdt :books :zeno/* :zeno/count]
                       num-msgs [:zeno/crdt :msgs :zeno/count]
                       book-nums [:zeno/crdt :books :zeno/* :nums :zeno/concat]
                       msgs [:zeno/crdt :msgs]
                       msg-indices [:zeno/crdt :msgs :zeno/keys]}
             update-fn (constantly nil)
             expected {'book-ids #{"123" "456" "789"}
                       'books-map books
                       'books-vals (set (vals books))
                       'num-books (count books)
                       'num-books-2 (count books)
                       'book-nums #{2 4 6 1 3 5 7}
                       'titles-1 titles-set
                       'titles-2 titles-set
                       'num-msgs 2
                       'msg-indices [0 1]
                       'msgs msgs}
             update-ret (au/<? (zc/<update-state!
                                zc [{:zeno/path [:zeno/crdt]
                                     :zeno/op :zeno/set
                                     :zeno/arg (u/sym-map books msgs)}]))
             sub-ret (zc/subscribe-to-state! zc "test" sub-map
                                             update-fn)]
         (is (= true update-ret))
         (is (= expected
                (-> sub-ret
                    (update 'book-ids set)
                    (update 'books-vals set)
                    (update 'book-nums set)
                    (update 'titles-1 set)
                    (update 'titles-2 set))))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e))
         (println (u/ex-msg-and-stacktrace e)))))))

(deftest test-empty-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/client :my-book-ids]
                       my-books [:zeno/client :books my-book-ids]}
             update-fn (constantly nil)
             expected '{my-book-ids nil
                        my-books nil}]
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :my-book-ids]
                                        :zeno/op :zeno/set
                                        :zeno/arg []}
                                       {:zeno/path [:zeno/client :books]
                                        :zeno/op :zeno/set
                                        :zeno/arg books}]))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                                 update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest  test-crdt-empty-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/crdt :my-book-ids]
                       my-books [:zeno/crdt :books my-book-ids]}
             update-fn (constantly nil)
             expected '{my-book-ids nil
                        my-books nil}]
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/crdt :my-book-ids]
                                        :zeno/op :zeno/set
                                        :zeno/arg []}
                                       {:zeno/path [:zeno/crdt :books]
                                        :zeno/op :zeno/set
                                        :zeno/arg books}]))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                                 update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-explicit-seq-in-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/client :books ["123" "789"] :title]}
             update-fn (constantly nil)
             expected {'my-titles ["Treasure Island" "Dr Jekyll and Mr Hyde"]}]
         (au/<? (zc/<set-state! zc [:zeno/client :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-explicit-seq-in-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/crdt :books ["123" "789"] :title]}
             update-fn (constantly nil)
             expected {'my-titles ["Treasure Island" "Dr Jekyll and Mr Hyde"]}]
         (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-in-path-1
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{num [:zeno/client :num]
                       title [:zeno/client :books num :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/client :books] books))
             _ (is (= true ret))
             expected {'num nil
                       'title nil}]
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-nil-in-path-1
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{num [:zeno/crdt :num]
                       title [:zeno/crdt :books num :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
             _ (is (= true ret))
             expected {'num nil
                       'title nil}]
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-in-path-2
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title [:zeno/client :books num :title]}
             resolution-map {'num nil}
             opts (u/sym-map resolution-map)
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/client :books] books))
             _ (is (= true ret))
             expected {'title nil}]
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn
                                                 opts)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-nil-in-path-2
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title [:zeno/crdt :books num :title]}
             resolution-map {'num nil}
             opts (u/sym-map resolution-map)
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
             _ (is (= true ret))
             expected {'title nil}]
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn
                                                 opts)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:zeno/client :books "999" :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/client :books] books))]
         (is (= true ret))
         (is (= {'title-999 nil}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:zeno/crdt :books "999" :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))]
         (is (= true ret))
         (is (= {'title-999 nil}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/client :books ["999"] :title]}
             update-fn (constantly nil)
             expected {'my-titles [nil]}]
         (au/<? (zc/<set-state! zc [:zeno/client :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/crdt :books ["999"] :title]}
             update-fn (constantly nil)
             expected {'my-titles [nil]}]
         (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-wildcard-sub
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             titles-set (set (map :title (vals books)))
             sub-map '{titles [:zeno/client :books :zeno/* :title]}
             update-fn #(ca/put! ch (update % 'titles set))
             ret0 (zc/subscribe-to-state! zc "test" sub-map
                                          update-fn)
             _ (is (= {'titles []} ret0))
             ret1 (au/<? (zc/<set-state! zc [:zeno/client :books] books))
             _ (is (= true ret1))
             _ (is (= {'titles titles-set} (au/<? ch)))
             ret2 (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :books "999"]
                                                 :zeno/op :zeno/set
                                                 :zeno/arg {:title "1984"}}]))
             _ (is (= true ret2))
             _ (is (= {'titles (conj titles-set "1984")} (au/<? ch)))
             ret3 (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :books "456"]
                                                 :zeno/op :zeno/remove}]))
             _ (is (= true ret3))
             expected-titles (-> (conj titles-set "1984")
                                 (disj "Kidnapped"))]
         (is (= {'titles expected-titles} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-crdt-wildcard-sub
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             titles-set (set (map :title (vals books)))
             sub-map '{titles [:zeno/crdt :books :zeno/* :title]}
             update-fn #(ca/put! ch (update % 'titles set))
             ret0 (zc/subscribe-to-state! zc "test" sub-map
                                          update-fn)
             _ (is (= {'titles []} ret0))
             ret1 (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
             _ (is (= true ret1))
             _ (is (= {'titles titles-set} (au/<? ch)))
             ret2 (au/<? (zc/<update-state!
                          zc
                          [{:zeno/path [:zeno/crdt :books "999"]
                            :zeno/op :zeno/set
                            :zeno/arg {:title "1984"}}]))
             _ (is (= true ret2))
             _ (is (= {'titles (conj titles-set "1984")} (au/<? ch)))
             ret3 (au/<? (zc/<update-state!
                          zc
                          [{:zeno/path [:zeno/crdt :books "456"]
                            :zeno/op :zeno/remove}]))
             _ (is (= true ret3))
             expected-titles (-> (conj titles-set "1984")
                                 (disj "Kidnapped"))]
         (is (= {'titles expected-titles} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-array-ops
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             id-to-fav-nums {1 [7 8 9]
                             2 [2 3 4]}
             ret1 (au/<? (zc/<set-state! zc
                                         [:zeno/client :id-to-fav-nums]
                                         id-to-fav-nums))
             _ (is (= true ret1))
             resolution-map {'id 2}
             sub-map '{my-nums [:zeno/client :id-to-fav-nums id]}
             update-fn #(ca/put! ch %)
             ret2 (zc/subscribe-to-state! zc "test" sub-map update-fn
                                          (u/sym-map resolution-map))
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/client :id-to-fav-nums 2 1]
                               :zeno/op :zeno/remove}]))
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 4]} (au/<? ch)))
             ret4 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/client :id-to-fav-nums 2 -1]
                               :zeno/op :zeno/insert-after
                               :zeno/arg 5}]))]
         (is (= true ret4))
         (is (= {'my-nums [2 4 5]} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest ^:this test-crdt-array-ops
  (au/test-async
   3000
   (ca/go
     (try
       (let [_ (log/info 1)
             crdt-schema (l/map-schema (l/array-schema l/int-schema))
             zc (c/->zc-unit (u/sym-map crdt-schema))
             ch (ca/chan 1)
             id-to-fav-nums {"1" [7 8 9]
                             "2" [2 3 4]}
             ret1 (au/<? (zc/<set-state! zc [:zeno/crdt] id-to-fav-nums))
             _ (log/info 2)
             _ (is (= true ret1))
             resolution-map {'id "2"}
             sub-map '{my-nums [:zeno/crdt id]}
             update-fn #(ca/put! ch %)
             ret2 (zc/subscribe-to-state! zc "test" sub-map update-fn
                                          (u/sym-map resolution-map))
             _ (log/info 3)
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/crdt "2" 1]
                               :zeno/op :zeno/remove}]))
             _ (log/info 4)
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 4]} (au/<? ch)))
             _ (log/info 5)
             ret4 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/crdt "2" -1]
                               :zeno/op :zeno/insert-after
                               :zeno/arg 5}]))
             _ (log/info 6)]
         (is (= true ret4))
         (is (= {'my-nums [2 4 5]} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-insert-range
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (c/->zc-unit)
             ch (ca/chan 1)
             id-to-fav-nums {"1" [7 8 9]
                             "2" [2 3 4]}
             ret1 (au/<? (zc/<set-state! zc [:zeno/client] id-to-fav-nums))
             _ (is (= true ret1))
             sub-map '{my-nums [:zeno/client "2"]}
             update-fn #(ca/put! ch %)
             ret2 (zc/subscribe-to-state! zc "test" sub-map update-fn)
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (zc/<update-state!
                          zc [{:zeno/arg [301 302 303]
                               :zeno/op :zeno/insert-range-after
                               :zeno/path [:zeno/client "2" 1]}]))
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 3 301 302 303 4]} (au/<? ch)))
             ret4 (au/<? (zc/<update-state!
                          zc [{:zeno/arg [304 305]
                               :zeno/op :zeno/insert-range-before
                               :zeno/path [:zeno/client "2" -1]}]))]
         (is (= true ret4))
         (is (= {'my-nums [2 3 301 302 303 304 305 4]} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-crdt-insert-range
  (au/test-async
   3000
   (ca/go
     (try
       (let [crdt-schema (l/map-schema (l/array-schema l/int-schema))
             zc (c/->zc-unit (u/sym-map crdt-schema))
             ch (ca/chan 1)
             id-to-fav-nums {"1" [7 8 9]
                             "2" [2 3 4]}
             ret1 (au/<? (zc/<set-state! zc [:zeno/crdt] id-to-fav-nums))
             _ (is (= true ret1))
             sub-map '{my-nums [:zeno/crdt "2"]}
             update-fn #(ca/put! ch %)
             ret2 (zc/subscribe-to-state! zc "test" sub-map update-fn)
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (zc/<update-state!
                          zc [{:zeno/arg [301 302 303]
                               :zeno/op :zeno/insert-range-after
                               :zeno/path [:zeno/crdt "2" 1]}]))
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 3 301 302 303 4]} (au/<? ch)))
             ret4 (au/<? (zc/<update-state!
                          zc [{:zeno/arg [304 305]
                               :zeno/op :zeno/insert-range-before
                               :zeno/path [:zeno/crdt "2" -1]}]))]
         (is (= true ret4))
         (is (= {'my-nums [2 3 301 302 303 304 305 4]} (au/<? ch)))
         (zc/stop! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))
