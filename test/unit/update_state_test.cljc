(ns unit.update-state-test
  (:require
   [clojure.core.async :as ca]
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.client :as zc]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(l/def-record-schema book-schema
  [:title l/string-schema])

(l/def-record-schema info-schema
  [:books (l/map-schema book-schema)])

(deftest test-bad-path-root-in-update-state!
  (au/test-async
   1000
   (ca/go
     (let [zc (zc/zeno-client)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Paths must begin with "
            (au/<? (zc/<update-state! zc [{:zeno/path [:not-a-valid-root :x]
                                           :zeno/op :zeno/set
                                           :zeno/arg 1}]))))
       (zc/shutdown! zc)))))

(deftest test-bad-command-op
  (au/test-async
   1000
   (ca/go
     (let [zc (zc/zeno-client)]
       (is (thrown-with-msg?
            #?(:clj ExceptionInfo :cljs js/Error)
            #"Invalid"
            (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :x]
                                           :zeno/op :not-an-op
                                           :zeno/arg 1}]))))
       (zc/shutdown! zc)))))

(deftest test-ordered-update-maps
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
             sub-map '{title [:zeno/client :msgs 0 :title]}
             ch (ca/chan 1)
             update-fn #(ca/put! ch (% 'title))
             orig-title "Plato"
             new-title "Socrates"]
         (is (= true (au/<? (zc/<update-state!
                             zc [{:zeno/path [:zeno/client]
                                  :zeno/op :zeno/set
                                  :zeno/arg {:msgs [{:title orig-title}]}}]))))
         (is (= {'title orig-title}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc
                                   [{:zeno/path [:zeno/client :msgs 0]
                                     :zeno/op :zeno/insert-before
                                     :zeno/arg {:title orig-title}}
                                    {:zeno/path [:zeno/client :msgs 0 :title]
                                     :zeno/op :zeno/set
                                     :zeno/arg new-title}]))
         (is (= new-title (au/<? ch)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :msgs 0 :title]
                                        :zeno/op :zeno/set
                                        :zeno/arg new-title}
                                       {:zeno/path [:zeno/client :msgs 0]
                                        :zeno/op :zeno/insert-before
                                        :zeno/arg {:title orig-title}}]))
         (is (= orig-title (au/<? ch)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-end-relative-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [orig-title "Foo"
             zc (zc/zeno-client)
             ch (ca/chan 1)
             sub-map '{last-title [:zeno/client :msgs -1 :title]}
             update-fn #(ca/put! ch (% 'last-title))
             new-title "Bar"
             ret (au/<? (zc/<update-state!
                         zc [{:zeno/path [:zeno/client]
                              :zeno/op :zeno/set
                              :zeno/arg {:msgs [{:title orig-title}]}}]))]
         (is (= true ret))
         (is (= {'last-title orig-title}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :msgs -1]
                                        :zeno/op :zeno/insert-after
                                        :zeno/arg {:title new-title}}]))
         (is (= new-title (au/<? ch)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-resolution-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-symbolic-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-sequence-join-in-sub-map-2
  ;; Tests changing state whose path depends on a different path in sub-map
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-set-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-kw-operators
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e))
         (println (u/ex-msg-and-stacktrace e)))))))

(deftest test-empty-sequence-join
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-book-ids [:zeno/client :my-book-ids]
                       my-books [:zeno/client :books my-book-ids]}
             update-fn (constantly nil)
             expected '{my-book-ids []
                        my-books nil}]
         (au/<? (zc/<update-state! zc [{:zeno/path [:zeno/client :my-book-ids]
                                        :zeno/op :zeno/set
                                        :zeno/arg []}
                                       {:zeno/path [:zeno/client :books]
                                        :zeno/op :zeno/set
                                        :zeno/arg books}]))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map
                                                 update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-explicit-seq-in-path
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/client :books ["123" "789"] :title]}
             update-fn (constantly nil)
             expected {'my-titles ["Treasure Island" "Dr Jekyll and Mr Hyde"]}]
         (au/<? (zc/<set-state! zc [:zeno/client :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-in-path-1
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-in-path-2
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-nil-in-path-2
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client {:crdt-schema info-schema})
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:zeno/client :books "999" :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/client :books] books))]
         (is (= true ret))
         (is (= {'title-999 nil}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client {:crdt-schema info-schema})
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{title-999 [:zeno/crdt :books "999" :title]}
             update-fn (constantly nil)
             ret (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))]
         (is (= true ret))
         (is (= {'title-999 nil}
                (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/client :books ["999"] :title]}
             update-fn (constantly nil)
             expected {'my-titles [nil]}]
         (au/<? (zc/<set-state! zc [:zeno/client :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-seq-w-nil-return
  (au/test-async
   1000
   (ca/go
     (try
       (let [zc (zc/zeno-client {:crdt-schema info-schema})
             books {"123" {:title "Treasure Island"}
                    "456" {:title "Kidnapped"}
                    "789" {:title "Dr Jekyll and Mr Hyde"}}
             sub-map '{my-titles [:zeno/crdt :books ["999"] :title]}
             update-fn (constantly nil)
             expected {'my-titles [nil]}]
         (au/<? (zc/<set-state! zc [:zeno/crdt :books] books))
         (is (= expected (zc/subscribe-to-state! zc "test" sub-map update-fn)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-wildcard-sub
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-crdt-wildcard-sub
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (zc/zeno-client {:crdt-schema info-schema})
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))

(deftest test-array-ops
  (au/test-async
   3000
   (ca/go
     (try
       (let [zc (zc/zeno-client)
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
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (is (= :unexpected e)))))))

(deftest test-crdt-array-ops
  (au/test-async
   3000
   (ca/go
     (try
       (let [crdt-schema (l/map-schema (l/array-schema l/int-schema))
             zc (zc/zeno-client (u/sym-map crdt-schema))
             ch (ca/chan 1)
             id-to-fav-nums {"1" [7 8 9]
                             "2" [2 3 4]}
             ret1 (au/<? (zc/<set-state! zc [:zeno/crdt] id-to-fav-nums))
             _ (is (= true ret1))
             resolution-map {'id "2"}
             sub-map '{my-nums [:zeno/crdt id]}
             update-fn #(ca/put! ch %)
             ret2 (zc/subscribe-to-state! zc "test" sub-map update-fn
                                          (u/sym-map resolution-map))
             _ (is (= {'my-nums [2 3 4]} ret2))
             ret3 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/crdt "2" 1]
                               :zeno/op :zeno/remove}]))
             _ (is (= true ret3))
             _ (is (= {'my-nums [2 4]} (au/<? ch)))
             ret4 (au/<? (zc/<update-state!
                          zc [{:zeno/path [:zeno/crdt "2" -1]
                               :zeno/op :zeno/insert-after
                               :zeno/arg 5}]))]
         (is (= true ret4))
         (is (= {'my-nums [2 4 5]} (au/<? ch)))
         (zc/shutdown! zc))
       (catch #?(:clj Exception :cljs js/Error) e
         (log/error (u/ex-msg-and-stacktrace e))
         (is (= :unexpected e)))))))
